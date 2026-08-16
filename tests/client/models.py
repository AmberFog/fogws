import asyncio
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LoopbackEndpoint:
    disconnected: asyncio.Event
    uri: str


@dataclass(frozen=True, slots=True)
class TerminalFrameEndpoint:
    trigger: asyncio.Event
    upgraded: asyncio.Event
    uri: str


@dataclass(frozen=True, slots=True)
class StalledHandshakeEndpoint:
    accepted: asyncio.Event
    disconnected: asyncio.Event
    uri: str


@dataclass(frozen=True, slots=True)
class UnresponsiveCloseEndpoint:
    close_received: asyncio.Event
    disconnected: asyncio.Event
    upgraded: asyncio.Event
    uri: str
