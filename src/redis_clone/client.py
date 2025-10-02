from __future__ import annotations

from typing import Any

from gevent import socket

from .protocol import Error, ProtocolHandler


class DisconnectError(Exception):
    pass


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 31337) -> None:
        self._protocol: ProtocolHandler = ProtocolHandler()
        self._socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh: Any = self._socket.makefile("rwb")

    def execute(self, *args: str) -> list[str] | Any:
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            # Return the error message directly instead of wrapping in CommandError
            return [resp.message]
        return resp

    def get(self, key: str) -> str | None | list[str]:
        return self.execute("GET", key)

    def set(self, key: str, value: str) -> int | list[str]:
        return self.execute("SET", key, value)

    def delete(self, key: str) -> int | list[str]:
        return self.execute("DELETE", key)

    def flush(self) -> int | list[str]:
        return self.execute("FLUSH")

    def mget(self, *keys: str) -> list[str | None] | list[str]:
        return self.execute("MGET", *keys)

    def mset(self, *keys: str) -> int | list[str]:
        return self.execute("MSET", *keys)
