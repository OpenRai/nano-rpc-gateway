#!/usr/bin/env python3
"""Verify deployed SSE session retirement using a private WS relay and xno-skills.

The relay must be the gateway's temporary WS upstream. This test spends exactly
three transfers of 0.00000001 XNO between the supplied agent-controlled wallets.
"""
import argparse
import json
import re
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


def report(**values):
    print(json.dumps(values), flush=True)


def http(base, path, body=None):
    request = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"User-Agent": "curl/8.21.0", "Content-Type": "application/json"},
    )
    try:
        # RPC can spend ten seconds on each of three ordered upstreams. The
        # observer's budget must exceed the gateway's complete failover budget.
        response = urllib.request.urlopen(request, timeout=45 if path == "/rpc" else 10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.headers, json.load(response)


def cli(*args):
    started = time.monotonic()
    result = subprocess.run(["xno-skills", *args, "--json"], capture_output=True, text=True)
    returned = time.monotonic()
    if result.returncode:
        raise RuntimeError(f"xno-skills {args[0]} failed; inspect before retrying: {result.stderr}")
    data = json.loads(result.stdout)
    report(command=args[0], durationMs=round((returned - started) * 1000, 3),
           powMs=[int(ms) for ms in re.findall(r"pow.generate ok elapsedMs=(\d+)", result.stderr)])
    return data, returned


class Stream:
    def __init__(self, base, destination, protocol, curl):
        self.protocol = protocol
        self.ready = threading.Event()
        self.ended = threading.Event()
        self.changed = threading.Condition()
        self.events = {}
        self.error = None
        self.stderr = tempfile.TemporaryFile(mode="w+t")
        self.process = subprocess.Popen([
            curl, "-f", "-sS", "-N", "--connect-timeout", "10", "--max-time", "600",
            "--http" + protocol, base + "/events/confirmations?accounts=" + destination,
        ], stdout=subprocess.PIPE, stderr=self.stderr, text=True, bufsize=1)
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            for line in self.process.stdout:
                if line.startswith(":") or line.startswith("event:"):
                    self.ready.set()
                if line.startswith("data:"):
                    event = json.loads(line[5:])["params"]
                    with self.changed:
                        self.events[event["hash"]] = (time.monotonic(), event)
                        self.changed.notify_all()
        except Exception as error:
            self.error = str(error)
        finally:
            self.process.wait()
            self.ended.set()
            with self.changed:
                self.changed.notify_all()

    def wait_ready(self):
        deadline = time.monotonic() + 35
        while not self.ready.wait(0.1):
            if self.ended.is_set() or time.monotonic() >= deadline:
                raise RuntimeError(f"HTTP/{self.protocol} subscription did not open")
        assert not self.ended.is_set(), f"HTTP/{self.protocol} ended before the test"

    def expect(self, block_hash, returned, destination, subtype):
        with self.changed:
            self.changed.wait_for(lambda: block_hash in self.events or self.ended.is_set(), timeout=5)
            assert block_hash in self.events, f"HTTP/{self.protocol} missed {block_hash}; ended={self.ended.is_set()}"
            observed, event = self.events[block_hash]
        assert event["amount"] == "10000000000000000000000"
        assert event["block"]["subtype"] == subtype
        if subtype == "send":
            assert event["block"]["link_as_account"] == destination
        else:
            assert event["account"] == destination
        delay = (observed - returned) * 1000
        assert delay <= 5000, f"confirmation took {delay}ms"
        report(protocol=self.protocol, hash=block_hash, subtype=subtype,
               sseAfterSuccessMs=round(delay, 3))

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
        self.thread.join(timeout=10)
        self.process.stdout.close()
        self.stderr.seek(0)
        report(protocol=self.protocol, curlExit=self.process.returncode, error=self.stderr.read().strip())
        self.stderr.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="https://dev-rpc.nanosweeper.lol")
    parser.add_argument("--docker-host", default="docker-lab.lan")
    parser.add_argument("--relay-container", default="nano-as3-ws-relay")
    parser.add_argument("--curl", default="curl")
    parser.add_argument("--source-wallet", required=True)
    parser.add_argument("--destination-wallet", required=True)
    parser.add_argument("--destination-address", required=True)
    parser.add_argument("--resume-send-hash", help="Reuse a first send already verified before an interrupted test; do not send it twice")
    args = parser.parse_args()
    base = args.gateway.rstrip("/")
    destination = args.destination_address
    wallets, _ = cli("wallets")
    assert any(w["name"] == args.source_wallet for w in wallets)
    assert any(w["name"] == args.destination_wallet and w["address"] == destination for w in wallets)
    valid, _ = cli("validate", destination)
    assert valid["valid"]
    if args.resume_send_hash:
        assert re.fullmatch(r"[0-9A-F]{64}", args.resume_send_hash)

    def relay(action):
        result = subprocess.run([
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", args.docker_host,
            "docker", "exec", args.relay_container, "curl", "-fsS", "--max-time", "5",
            "-X", "POST", "http://127.0.0.1:8081/" + action,
        ], capture_output=True, text=True, check=True)
        report(relay=action, response=json.loads(result.stdout))

    def await_ready():
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if http(base, "/readyz")[0] == 200:
                return
            time.sleep(0.5)
        raise RuntimeError("upstream did not recover within 30s")

    streams = []
    paused = False
    sends = []

    def open_streams():
        opened = [Stream(base, destination, protocol, args.curl) for protocol in ("1.1", "2")]
        streams.extend(opened)
        for stream in opened:
            stream.wait_ready()
        return opened

    def send(index, active):
        balance, _ = cli("balance", "--wallet", args.source_wallet)
        assert int(balance["balanceRaw"]) >= 10000000000000000000000
        report(send=index, balanceRaw=balance["balanceRaw"], pendingRaw=balance["pendingRaw"])
        sent, returned = cli("send", "--wallet", args.source_wallet, "--to", destination,
                             "--amount-xno", "0.00000001")
        sends.append(sent["hash"])
        report(send=index, hash=sent["hash"])
        for stream in active:
            stream.expect(sent["hash"], returned, destination, "send")

    try:
        await_ready()
        # Check the independent RPC path before introducing any WS failure.
        status, _, response = http(base, "/rpc", {
            "jsonrpc":"2.0", "id":0, "method":"account_info", "params":{"account":destination}
        })
        assert status == 200 and "result" in response, "baseline RPC read failed before fault injection"
        original = open_streams()
        if args.resume_send_hash:
            sends.append(args.resume_send_hash)
            report(resumedFirstSend=args.resume_send_hash)
        else:
            send(1, original)
        assert all(not stream.ended.is_set() for stream in original)
        paused = True
        relay("pause")
        failed_at = time.monotonic()
        for stream in original:
            assert stream.ended.wait(max(0, 5 - (time.monotonic() - failed_at))), "old stream did not terminate"
            report(protocol=stream.protocol, oldStreamClosedMs=round((time.monotonic() - failed_at) * 1000, 3))
        assert http(base, "/readyz")[0] == 503
        for body in (None, {}):
            status, headers, response = http(base, "/events/confirmations", body)
            assert status == 503 and headers.get("Retry-After") == "2"
            assert headers.get("Cache-Control") == "no-store"
            assert response["status"] == "not_ready"
        status, _, response = http(base, "/rpc", {
            "jsonrpc":"2.0", "id":1, "method":"account_info", "params":{"account":destination}
        })
        assert status == 200 and "result" in response, "RPC failed during WS outage"
        report(outage503=True, rpcDuringOutage=True)
        time.sleep(max(0, 5 - (time.monotonic() - failed_at)))
        relay("resume")
        paused = False
        await_ready()
        replacement = open_streams()
        # Subscribe first; reconcile before applying later events.
        for method, params in (
            ("accounts_frontiers", {"accounts":[destination]}),
            ("account_history", {"account":destination, "count":"10"}),
        ):
            status, _, response = http(base, "/rpc", {"jsonrpc":"2.0", "id":2, "method":method, "params":params})
            assert status == 200 and "result" in response
        assert all(stream.ended.is_set() for stream in original)
        send(2, replacement)
        send(3, replacement)
        for block_hash in sends:
            received, returned = cli("receive", "--wallet", args.destination_wallet, "--hash", block_hash)
            block = next(block for block in received["received"] if block["sendHash"] == block_hash)
            for stream in replacement:
                stream.expect(block["hash"], returned, destination, "receive")
        balance, _ = cli("balance", "--wallet", args.destination_wallet)
        report(result="PASS", sends=sends, finalBalance=balance)
    finally:
        try:
            if paused:
                relay("resume")
        finally:
            for stream in streams:
                stream.close()


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise SystemExit("verification interrupted; restoring relay")
    signal.signal(signal.SIGTERM, interrupted)
    main()
