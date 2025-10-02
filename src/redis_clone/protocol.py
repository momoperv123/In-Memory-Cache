from __future__ import annotations

from collections import namedtuple
from io import BytesIO
from typing import Any, Callable


class CommandError(Exception):
    pass


class DisconnectError(Exception):
    pass


Error = namedtuple("Error", ("message",))


class ProtocolHandler:
    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[Any], Any]] = {
            "+": self.handle_simple_string,
            "-": self.handle_error,
            ":": self.handle_integer,
            "$": self.handle_string,
            "*": self.handle_array,
            "%": self.handle_dict,
        }

    def handle_request(
        self, socket_file: Any
    ) -> str | Error | int | None | list[Any] | dict[str, Any]:
        first_byte = socket_file.read(1)

        if not first_byte:
            raise DisconnectError()

        first_byte = first_byte.decode("utf-8")
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError as e:
            raise CommandError("Bad Request") from e

    def handle_simple_string(self, socket_file: Any) -> str:
        return socket_file.readline().rstrip(b"\r\n").decode("utf-8")

    def handle_error(self, socket_file: Any) -> Error:
        return Error(socket_file.readline().rstrip(b"\r\n").decode("utf-8"))

    def handle_integer(self, socket_file: Any) -> int:
        return int(socket_file.readline().rstrip(b"\r\n"))

    def handle_string(self, socket_file: Any) -> str | None:
        length = int(socket_file.readline().rstrip(b"\r\n"))

        if length == -1:
            return None

        data = socket_file.read(length + 2)[:-2]
        return data.decode("utf-8")

    def handle_array(self, socket_file: Any) -> list[Any]:
        num_elements = int(socket_file.readline().rstrip(b"\r\n"))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file: Any) -> dict[str, Any]:
        num_items = int(socket_file.readline().rstrip(b"\r\n"))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file: Any, data: Any) -> None:
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf: BytesIO, data: Any) -> None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        if isinstance(data, bytes):
            buf.write(b"$%d\r\n" % len(data))
            buf.write(data)
            buf.write(b"\r\n")
        elif isinstance(data, int):
            buf.write(b":%d\r\n" % data)
        elif isinstance(data, Error):
            msg = (data.message or "").encode("utf-8")
            buf.write(b"-")
            buf.write(msg)
            buf.write(b"\r\n")
        elif isinstance(data, (list, tuple)) and not isinstance(data, Error):
            buf.write(b"*%d\r\n" % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b"%%%d\r\n" % len(data))
            for key, val in data.items():
                self._write(buf, key)
                self._write(buf, val)
        elif data is None:
            buf.write(b"$-1\r\n")
        else:
            raise CommandError("Unrecognized type: %s", type(data))
