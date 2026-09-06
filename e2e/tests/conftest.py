from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

CLIENT_ROOT = (
    Path(__file__).resolve().parents[1]
    / "generated"
    / "nano_gateway_client"
    / "python"
    / "nano-gateway-http-client"
)
if str(CLIENT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(CLIENT_ROOT))

from nano_gateway_http_client.client import NanoGatewayClient  # noqa: E402


@dataclass(frozen=True)
class Settings:
    rpc_url: str
    events_url: str
    wallet_rpc_url: str | None
    bearer_token: str | None
    wallet_a: str
    wallet_b: str
    account_a: str | None
    account_b: str | None
    transfer_xno: str
    run_live: bool
    run_mutations: bool

    @classmethod
    def from_environment(cls) -> Settings:
        rpc_url = os.getenv("NANO_E2E_RPC_URL", "http://127.0.0.1:8090/rpc").rstrip("/")
        events_url = os.getenv(
            "NANO_E2E_EVENTS_URL",
            f"{rpc_url.removesuffix('/rpc')}/events/confirmations",
        )
        return cls(
            rpc_url=rpc_url,
            events_url=events_url,
            wallet_rpc_url=os.getenv("NANO_E2E_WALLET_RPC_URL"),
            bearer_token=os.getenv("NANO_E2E_BEARER_TOKEN"),
            wallet_a=os.getenv("NANO_E2E_WALLET_A", "nano-gateway-e2e-a"),
            wallet_b=os.getenv("NANO_E2E_WALLET_B", "nano-gateway-e2e-b"),
            account_a=os.getenv("NANO_E2E_ACCOUNT_A"),
            account_b=os.getenv("NANO_E2E_ACCOUNT_B"),
            transfer_xno=os.getenv("NANO_E2E_TRANSFER_XNO", "0.000001"),
            run_live=os.getenv("NANO_E2E_RUN_LIVE", "0") == "1",
            run_mutations=os.getenv("NANO_E2E_RUN_MUTATIONS", "0") == "1",
        )

    @property
    def headers(self) -> dict[str, str]:
        if self.bearer_token:
            return {"Authorization": f"Bearer {self.bearer_token}"}
        return {}


class WalletCli:
    """Small, secret-free adapter around the xno-skills CLI."""

    def __init__(self, rpc_url: str | None = None) -> None:
        executable = shutil.which("xno-skills")
        if executable is None:
            raise RuntimeError("xno-skills CLI is not installed")
        self.executable = executable
        self.rpc_url = rpc_url

    def _run(self, *args: str) -> Any:
        environment = os.environ.copy()
        if self.rpc_url:
            environment["NANO_RPC_URL"] = self.rpc_url
        completed = subprocess.run(
            [self.executable, *args],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
            timeout=180,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"xno-skills {' '.join(args[:2])} failed")
        decoder = json.JSONDecoder()
        candidates: list[tuple[int, Any]] = []
        for index, character in enumerate(completed.stdout):
            if character not in "[{":
                continue
            try:
                value, end = decoder.raw_decode(completed.stdout[index:])
                candidates.append((index + end, value))
            except json.JSONDecodeError:
                pass
        return max(candidates, key=lambda candidate: candidate[0])[1] if candidates else None

    def wallet_address(self, name: str) -> str:
        wallets = self._run("wallets", "--json")
        if not isinstance(wallets, list):
            raise RuntimeError("xno-skills wallets returned no JSON list")
        for wallet in wallets:
            if isinstance(wallet, dict) and wallet.get("name") == name:
                address = wallet.get("address")
                if isinstance(address, str) and address.startswith("nano_"):
                    return address
        raise RuntimeError(f"xno-skills wallet not found: {name}")

    def balance(self, name: str) -> dict[str, Any]:
        result = self._run("balance", "--wallet", name, "--json")
        if not isinstance(result, dict):
            raise RuntimeError("xno-skills balance returned no JSON object")
        return result

    def send(self, name: str, destination: str, amount_xno: str) -> dict[str, Any]:
        result = self._run(
            "send",
            "--wallet",
            name,
            "--to",
            destination,
            "--amount-xno",
            amount_xno,
            "--json",
        )
        if not isinstance(result, dict):
            raise RuntimeError("xno-skills send returned no JSON object")
        return result

    def receive(self, name: str) -> dict[str, Any] | None:
        result = self._run("receive", "--wallet", name, "--json")
        return result if isinstance(result, dict) else None


def model_dict(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, list):
        return [model_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: model_dict(item) for key, item in value.items()}
    return value


def extract_hash(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("hash", "blockHash", "block_hash"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
        for nested in value.values():
            try:
                return extract_hash(nested)
            except AssertionError:
                pass
    elif isinstance(value, list):
        for nested in value:
            try:
                return extract_hash(nested)
            except AssertionError:
                pass
    raise AssertionError("wallet command did not return a block hash")


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_environment()


@pytest.fixture
async def http(settings: Settings):
    if not settings.run_live:
        pytest.skip("set NANO_E2E_RUN_LIVE=1 to run live gateway tests")
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            response = await client.get(f"{settings.rpc_url.removesuffix('/rpc')}/health")
        except httpx.HTTPError as exc:
            pytest.skip(f"gateway is unavailable at {settings.rpc_url}: {exc}")
        if response.status_code != 200:
            pytest.skip(f"gateway health check returned HTTP {response.status_code}")
        yield client


@pytest.fixture
async def rpc_client(settings: Settings, http: httpx.AsyncClient) -> NanoGatewayClient:
    return NanoGatewayClient(headers=settings.headers, client_url=settings.rpc_url, timeout=20)


@pytest.fixture
def wallet_cli(settings: Settings) -> WalletCli:
    try:
        return WalletCli(settings.wallet_rpc_url)
    except RuntimeError as exc:
        pytest.skip(str(exc))


@pytest.fixture
def accounts(settings: Settings, wallet_cli: WalletCli) -> tuple[str, str]:
    try:
        account_a = settings.account_a or wallet_cli.wallet_address(settings.wallet_a)
        account_b = settings.account_b or wallet_cli.wallet_address(settings.wallet_b)
    except RuntimeError as exc:
        pytest.skip(str(exc))
    return account_a, account_b


async def rpc_json(
    http: httpx.AsyncClient,
    settings: Settings,
    method: str,
    params: dict[str, Any] | None = None,
    request_id: int = 1,
) -> dict[str, Any]:
    body: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": request_id}
    if params is not None:
        body["params"] = params
    response = await http.post(settings.rpc_url, json=body, headers=settings.headers)
    response.raise_for_status()
    payload = response.json()
    assert payload.get("jsonrpc") == "2.0"
    return payload
