"""Python client template."""

import datetime
from typing import Any, Literal, Union
from uuid import UUID

from jsonrpc2pyclient.decorator import Transportable, rpc_class_method
from jsonrpc2pyclient.httpclient import AsyncRPCHTTPClient
from jsonrpc2pyclient.rpcclient import AsyncRPCClient, RPCClient
from py_undefined import Undefined
from pydantic import UUID1, UUID3, UUID4, UUID5

from nano_gateway_http_client.models import (
    AccountBalanceResult,
    AccountHistoryResult,
    AccountInfoResult,
    BlockCountResult,
    BlockInfoResult,
    BlocksInfoResult,
    ProcessResult,
    ReceivableResult,
    VersionResult,
)

ClientType = Union[AsyncRPCClient, RPCClient]
CLIENT_URL = "http://127.0.0.1:8090/rpc"


class NanoGatewayClient(Transportable):
    def __init__(
        self, headers: dict[str, Any], client_url: str = CLIENT_URL, **kwargs: Any
    ) -> None:
        transport = AsyncRPCHTTPClient(client_url, headers, **kwargs)
        self.rpc = NanoGatewayClient._RpcClient(transport)
        super().__init__(transport)

    @rpc_class_method(method_name="version")
    async def version(self) -> VersionResult: ...
    @rpc_class_method(method_name="block_count")
    async def block_count(self) -> BlockCountResult: ...
    @rpc_class_method(method_name="account_info")
    async def account_info(self, account: str) -> AccountInfoResult: ...
    @rpc_class_method(method_name="receivable")
    async def receivable(self, account: str) -> ReceivableResult: ...
    @rpc_class_method(method_name="account_balance")
    async def account_balance(self, account: str) -> AccountBalanceResult: ...
    @rpc_class_method(method_name="account_history")
    async def account_history(
        self, account: str, count: int = Undefined
    ) -> AccountHistoryResult: ...
    @rpc_class_method(method_name="block_info")
    async def block_info(self, hash: str) -> BlockInfoResult: ...
    @rpc_class_method(method_name="blocks_info")
    async def blocks_info(self, hashes: list[str]) -> BlocksInfoResult: ...
    @rpc_class_method(method_name="process")
    async def process(self, block: dict[str, Any]) -> ProcessResult: ...

    class _RpcClient(Transportable):
        @rpc_class_method(method_name="rpc.discover")
        async def discover(self) -> dict[str, Any]: ...
