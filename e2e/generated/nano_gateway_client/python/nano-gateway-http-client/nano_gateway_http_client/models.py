from __future__ import annotations

from uuid import UUID
import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, UUID1, UUID3, UUID4, UUID5


class AccountBalanceResult(BaseModel):
    balance: str
    receivable: str


class AccountHistoryParams(BaseModel):
    account: str
    count: int


class AccountHistoryResult(BaseModel):
    account: str
    history: list[Any]


class AccountInfoResult(BaseModel):
    balance: str
    confirmation_height: str
    confirmation_height_frontier: str | None
    confirmed_balance: str
    confirmed_frontier: str | None
    frontier: str | None
    open_block: str | None
    opened: bool
    representative_block: str | None


class AccountParams(BaseModel):
    account: str


class BlockCountResult(BaseModel):
    cemented: str
    count: str
    unchecked: str


class BlockInfoResult(BaseModel):
    amount: str
    balance: str
    block: dict[str, Any]
    block_account: str
    confirmed: bool
    hash: str
    height: str
    subtype: str


class BlockParams(BaseModel):
    hash: str


class BlocksInfoResult(BaseModel):
    blocks: dict[str, BlockInfoResult]


class BlocksParams(BaseModel):
    hashes: list[str]


class ProcessParams(BaseModel):
    block: dict[str, Any]


class ProcessResult(BaseModel):
    hash: str


class ReceivableEntry(BaseModel):
    amount: str
    hash: str
    source: str


class VersionResult(BaseModel):
    rpc_version: str


class WorkGenerateParams(BaseModel):
    difficulty: str
    hash: str


class WorkGenerateResult(BaseModel):
    difficulty: str
    hash: str
    multiplier: str
    work: str


AccountBalanceResult.model_rebuild()
AccountHistoryParams.model_rebuild()
AccountHistoryResult.model_rebuild()
AccountInfoResult.model_rebuild()
AccountParams.model_rebuild()
BlockCountResult.model_rebuild()
BlockInfoResult.model_rebuild()
BlockParams.model_rebuild()
BlocksInfoResult.model_rebuild()
BlocksParams.model_rebuild()
ProcessParams.model_rebuild()
ProcessResult.model_rebuild()
ReceivableEntry.model_rebuild()
VersionResult.model_rebuild()
WorkGenerateParams.model_rebuild()
WorkGenerateResult.model_rebuild()

ReceivableResult = list[ReceivableEntry]
