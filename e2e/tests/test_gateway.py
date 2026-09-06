from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import (
    Settings,
    WalletCli,
    extract_hash,
    model_dict,
    rpc_json,
)

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "generated" / "nano-node-v28.2.openrpc.json"
CLIENT_PATH = (
    ROOT
    / "e2e"
    / "generated"
    / "nano_gateway_client"
    / "python"
    / "nano-gateway-http-client"
    / "nano_gateway_http_client"
    / "client.py"
)


@dataclass
class SseEvent:
    event: str | None
    event_id: str | None
    data: Any


class SseReader:
    def __init__(self, response: httpx.Response) -> None:
        self._lines = response.aiter_lines()

    async def next_event(self) -> SseEvent:
        event_name: str | None = None
        event_id: str | None = None
        data_lines: list[str] = []
        async for line in self._lines:
            if line == "":
                if event_name is None and event_id is None and not data_lines:
                    continue
                raw_data = "\n".join(data_lines)
                try:
                    data: Any = json.loads(raw_data)
                except json.JSONDecodeError:
                    data = raw_data
                return SseEvent(event_name, event_id, data)
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if field == "event":
                event_name = value
            elif field == "id":
                event_id = value
            elif field == "data":
                data_lines.append(value)
        raise AssertionError("SSE stream closed before an event frame")


async def confirmation_for_hash(reader: SseReader, expected_hash: str) -> SseEvent:
    while True:
        event = await reader.next_event()
        if event.event == "nano.stream_reset":
            raise AssertionError("confirmation stream reset before the expected hash")
        assert event.event == "nano.confirmation"
        assert isinstance(event.event_id, str)
        assert event.event_id
        assert isinstance(event.data, dict)
        if event.data.get("hash") == expected_hash:
            return event


async def next_sse_event(response: httpx.Response) -> SseEvent:
    return await SseReader(response).next_event()


def assert_nonempty_strings(value: Any, fields: tuple[str, ...]) -> None:
    item = model_dict(value)
    assert isinstance(item, dict)
    for field in fields:
        assert isinstance(item.get(field), str), field
        assert item[field], field


def test_openrpc_artifact_matches_generated_python_client() -> None:
    schema = json.loads(SCHEMA_PATH.read_text())
    generated = CLIENT_PATH.read_text()
    schema_methods = {method["name"] for method in schema["methods"]}
    generated_methods = {
        line.split('method_name="', 1)[1].split('"', 1)[0]
        for line in generated.splitlines()
        if 'method_name="' in line
    }
    assert schema_methods == generated_methods
    digest = schema["x-nano-artifact-sha256"]
    assert (
        ROOT / "e2e" / "generated" / "nano_gateway_client" / "source-digest.txt"
    ).read_text().strip() == digest


@pytest.mark.live
async def test_health_and_readiness(http: httpx.AsyncClient, settings: Settings) -> None:
    health = await http.get(f"{settings.rpc_url.removesuffix('/rpc')}/health")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    ready = await http.get(f"{settings.rpc_url.removesuffix('/rpc')}/readyz")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


@pytest.mark.live
async def test_rpc_discover_and_openrpc_document(
    http: httpx.AsyncClient, rpc_client, settings: Settings
) -> None:
    response = await http.get(f"{settings.rpc_url.removesuffix('/rpc')}/openrpc.json")
    response.raise_for_status()
    document = response.json()
    discovered = model_dict(await rpc_client.rpc.discover())
    assert discovered["openrpc"] == "1.3.2"
    assert discovered["x-nano-artifact-sha256"] == document["x-nano-artifact-sha256"]
    assert [method["name"] for method in discovered["methods"]] == [
        method["name"] for method in document["methods"]
    ]


@pytest.mark.live
async def test_jsonrpc_invalid_request_and_method_not_found_errors(
    http: httpx.AsyncClient, settings: Settings
) -> None:
    invalid = await rpc_json(http, settings, "account_info", {})
    assert invalid["error"]["code"] == -32602
    assert isinstance(invalid["error"]["message"], str)

    unknown = await rpc_json(http, settings, "not_a_gateway_method")
    assert unknown["error"]["code"] == -32601
    assert isinstance(unknown["error"]["message"], str)


@pytest.mark.live
async def test_confirmation_sse_replay_reset_event(
    http: httpx.AsyncClient, settings: Settings
) -> None:
    headers = {**settings.headers, "Last-Event-ID": "unknown-generation:1"}
    async with http.stream("GET", settings.events_url, headers=headers) as stream:
        stream.raise_for_status()
        reset = await asyncio.wait_for(next_sse_event(stream), timeout=20)
    assert reset.event == "nano.stream_reset"
    assert isinstance(reset.data, str)
    assert "reconcile" in reset.data


@pytest.mark.live
async def test_version(rpc_client) -> None:
    result = model_dict(await rpc_client.version())
    assert_nonempty_strings(result, ("rpc_version",))


@pytest.mark.live
async def test_block_count(rpc_client) -> None:
    result = model_dict(await rpc_client.block_count())
    assert_nonempty_strings(result, ("count", "unchecked", "cemented"))


@pytest.mark.live
async def test_account_info(rpc_client, accounts) -> None:
    account_a, _ = accounts
    result = model_dict(await rpc_client.account_info(account_a))
    assert isinstance(result["opened"], bool)
    assert_nonempty_strings(result, ("balance", "confirmed_balance", "confirmation_height"))
    assert result["frontier"] is None or isinstance(result["frontier"], str)


@pytest.mark.live
async def test_receivable(rpc_client, accounts) -> None:
    _, account_b = accounts
    result = model_dict(await rpc_client.receivable(account_b))
    assert isinstance(result, list)
    for entry in result:
        assert_nonempty_strings(entry, ("source", "hash", "amount"))


@pytest.mark.live
async def test_account_balance(rpc_client, accounts) -> None:
    account_a, _ = accounts
    result = model_dict(await rpc_client.account_balance(account_a))
    assert_nonempty_strings(result, ("balance", "receivable"))


@pytest.mark.live
async def test_account_history(rpc_client, accounts) -> None:
    account_a, _ = accounts
    account_info = model_dict(await rpc_client.account_info(account_a))
    if not account_info["opened"]:
        pytest.skip("wallet A is unopened; fund and receive it before live tests")
    result = model_dict(await rpc_client.account_history(account_a, count=20))
    assert result["account"] == account_a
    assert isinstance(result["history"], list)


@pytest.mark.live
async def test_block_info_and_blocks_info(rpc_client, accounts) -> None:
    account_a, _ = accounts
    account = model_dict(await rpc_client.account_info(account_a))
    frontier = account.get("frontier")
    if not frontier:
        pytest.skip("wallet A has no frontier; fund and receive it before live tests")

    block = model_dict(await rpc_client.block_info(frontier))
    assert_nonempty_strings(
        block, ("hash", "block_account", "amount", "balance", "height", "subtype")
    )
    assert isinstance(block["confirmed"], bool)
    assert isinstance(block["block"], dict)

    blocks = model_dict(await rpc_client.blocks_info([frontier]))
    assert frontier in blocks["blocks"]
    assert blocks["blocks"][frontier]["hash"] == frontier


@pytest.mark.live
async def test_process_response_is_stable(
    rpc_client, http: httpx.AsyncClient, settings: Settings
) -> None:
    process_block = None
    raw_block = os.getenv("NANO_E2E_PROCESS_BLOCK")
    if raw_block:
        process_block = json.loads(raw_block)
        result = model_dict(await rpc_client.process(process_block))
        assert_nonempty_strings(result, ("hash",))
        return

    payload = await rpc_json(http, settings, "process", {"block": {"type": "state"}})
    assert "error" in payload
    assert payload["error"]["code"] in {-32010, -32001, -32000}
    assert isinstance(payload["error"]["message"], str)


@pytest.mark.live
async def test_work_generate_is_disabled_or_authorized(
    http: httpx.AsyncClient, settings: Settings
) -> None:
    payload = await rpc_json(
        http,
        settings,
        "work_generate",
        {"hash": "A", "difficulty": "C"},
    )
    if "result" in payload:
        assert_nonempty_strings(payload["result"], ("hash", "work", "difficulty", "multiplier"))
        return
    assert "error" in payload
    assert payload["error"]["code"] in {-32604, -32001, -32000}


@pytest.mark.live
@pytest.mark.mutation
async def test_confirmation_sse_send_receive_and_reconcile(
    http: httpx.AsyncClient,
    settings: Settings,
    accounts: tuple[str, str],
    wallet_cli: WalletCli,
    rpc_client,
) -> None:
    if not settings.run_mutations:
        pytest.skip("set NANO_E2E_RUN_MUTATIONS=1 to submit wallet transactions")
    account_a, account_b = accounts
    before = model_dict(await rpc_client.account_balance(account_b))
    request = http.build_request("GET", settings.events_url, params={"accounts": account_b})

    async with http.stream(request.method, request.url, headers=settings.headers) as stream:
        stream.raise_for_status()
        events = SseReader(stream)
        sent = await asyncio.to_thread(
            wallet_cli.send,
            settings.wallet_a,
            account_b,
            settings.transfer_xno,
        )
        sent_hash = extract_hash(sent)
        sent_event = await asyncio.wait_for(confirmation_for_hash(events, sent_hash), timeout=120)

        assert sent_event.event == "nano.confirmation"
        sent_data = sent_event.data
        assert sent_data["hash"] == sent_hash
        assert sent_data["profile"] == "nano-node/V28.2"
        assert sent_data["account"] == account_a
        assert sent_data["destination"] == account_b
        assert sent_data["block"]["subtype"] == "send"

        received = await asyncio.to_thread(wallet_cli.receive, settings.wallet_b)
        assert received is not None
        received_hash = extract_hash(received)
        receive_event = await asyncio.wait_for(
            confirmation_for_hash(events, received_hash), timeout=120
        )

    assert receive_event.event == "nano.confirmation"
    receive_data = receive_event.data
    assert receive_data["hash"] != sent_hash
    assert receive_data["hash"] == received_hash
    assert receive_data["profile"] == "nano-node/V28.2"
    assert receive_data["account"] == account_b
    assert receive_data["block"]["subtype"] == "receive"

    after_info = model_dict(await rpc_client.account_info(account_b))
    after_balance = model_dict(await rpc_client.account_balance(account_b))
    after_receivable = model_dict(await rpc_client.receivable(account_b))
    after_history = model_dict(await rpc_client.account_history(account_b, count=20))
    assert int(after_balance["balance"]) > int(before["balance"])
    assert after_info["confirmed_balance"] == after_balance["balance"]
    assert after_balance["receivable"] == "0"
    assert after_receivable == []
    assert after_history["account"] == account_b
    history_hashes = {
        item.get("hash") for item in after_history["history"] if isinstance(item, dict)
    }
    assert receive_data["hash"] in history_hashes
