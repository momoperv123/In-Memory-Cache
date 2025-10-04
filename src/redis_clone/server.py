from __future__ import annotations

from typing import Any

from gevent import monkey
from gevent.pool import Pool
from gevent.server import StreamServer

from .aof import AOFManager, FsyncPolicy
from .errors import ArityError, CommandError, WrongTypeError
from .protocol import Error, ProtocolHandler
from .ttl import TTLManager


class DisconnectError(Exception):
    pass


class Server:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 31337,
        max_clients: int = 64,
        aof_file: str = "redis_clone.aof",
        fsync_policy: FsyncPolicy = FsyncPolicy.EVERYSEC,
    ) -> None:
        self._pool: Pool = Pool(max_clients)
        self._server: StreamServer = StreamServer(
            (host, port), self.connection_handler, spawn=self._pool
        )
        self._protocol: ProtocolHandler = ProtocolHandler()
        self._kv: dict[str, str] = {}
        self._ttl_manager: TTLManager = TTLManager()
        self._aof_manager: AOFManager = AOFManager(aof_file, fsync_policy)
        self._commands: dict[str, Any] = self.get_commands()

        self._aof_manager.start()
        commands_replayed = self._aof_manager.replay_commands(self._replay_command)
        if commands_replayed > 0:
            print(f"Replayed {commands_replayed} commands from AOF")

    def _replay_command(self, command: str, *args: str) -> None:
        
        if command in self._commands:
            self._commands[command](*args)

    def _log_command(self, command: str, *args: str) -> None:
        
        self._aof_manager.append_command(command, *args)

    def get_commands(self) -> dict[str, Any]:
        return {
            "GET": self.get,
            "SET": self.set,
            "DELETE": self.delete,
            "FLUSH": self.flush,
            "MGET": self.mget,
            "MSET": self.mset,
            "EXPIRE": self.expire,
            "PEXPIRE": self.pexpire,
            "TTL": self.ttl,
            "PTTL": self.pttl,
            "EXISTS": self.exists,
            "KEYS": self.keys,
        }

    def get(self, key: str) -> str | None:
        if key not in self._kv:
            return None

        if self._ttl_manager.is_expired(key):
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            return None

        return self._kv.get(key)

    def set(self, key: str, value: str) -> int:
        self._kv[key] = value
        self._ttl_manager.remove_ttl(key)
        self._log_command("SET", key, value)
        return 1

    def delete(self, key: str) -> int:
        if key in self._kv:
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            self._log_command("DELETE", key)
            return 1
        return 0

    def flush(self) -> int:
        kvlen = len(self._kv)
        self._kv.clear()
        self._ttl_manager.clear()
        self._log_command("FLUSH")
        return kvlen

    def mget(self, *keys: str) -> list[str | None]:
        res = []
        for key in keys:
            if key not in self._kv:
                res.append(None)
            elif self._ttl_manager.is_expired(key):
                del self._kv[key]
                self._ttl_manager.remove_ttl(key)
                res.append(None)
            else:
                res.append(self._kv.get(key))
        return res

    def mset(self, *items: str) -> int:
        pairs = list(zip(items[::2], items[1::2]))
        for key, value in pairs:
            self._kv[key] = value
            self._ttl_manager.remove_ttl(key)
        self._log_command("MSET", *items)
        return len(pairs)

    def expire(self, key: str, seconds: int) -> int:
        if key not in self._kv:
            return 0
        if seconds == 0:
            # Immediate expiration - delete the key
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            self._log_command("EXPIRE", key, str(seconds))
            return 1
        result = 1 if self._ttl_manager.set_expiry(key, seconds * 1000) else 0
        if result:
            self._log_command("EXPIRE", key, str(seconds))
        return result

    def pexpire(self, key: str, milliseconds: int) -> int:
        if key not in self._kv:
            return 0
        if milliseconds == 0:
            # Immediate expiration - delete the key
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            self._log_command("PEXPIRE", key, str(milliseconds))
            return 1
        result = 1 if self._ttl_manager.set_expiry(key, milliseconds) else 0
        if result:
            self._log_command("PEXPIRE", key, str(milliseconds))
        return result

    def ttl(self, key: str) -> int:
        if key not in self._kv:
            return -2  # Key doesn't exist

        if self._ttl_manager.is_expired(key):
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            return -2  # Key expired and removed

        ttl_ms = self._ttl_manager.get_ttl(key)
        if ttl_ms == -1:
            return -1  # Key exists but has no TTL
        return max(1, ttl_ms // 1000)

    def pttl(self, key: str) -> int:
        if key not in self._kv:
            return -2  # Key doesn't exist

        if self._ttl_manager.is_expired(key):
            del self._kv[key]
            self._ttl_manager.remove_ttl(key)
            return -2  # Key expired and removed

        return self._ttl_manager.get_ttl(key)

    def exists(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._kv and not self._ttl_manager.is_expired(key):
                count += 1
        return count

    def keys(self, pattern: str = "*") -> list[str]:
        if pattern != "*":
            raise CommandError(f"Pattern '{pattern}' not supported")

        # Force cleanup of expired keys and get the list of expired keys
        expired_keys = self._ttl_manager.cleanup_expired(force=True)

        # Remove expired keys from the main dictionary
        for key in expired_keys:
            if key in self._kv:
                del self._kv[key]

        return list(self._kv.keys())

    def get_response(self, data: list[str] | str | bytes) -> Any:
        if not isinstance(data, list):
            try:
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                data = data.split()
            except Exception as e:
                raise CommandError("Request must be list or simple string") from e

        if not data:
            raise CommandError("Missing command")

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(command)

        args = data[1:]
        if command == "GET" and len(args) != 1:
            raise ArityError(command, 1, len(args))
        elif command == "SET" and len(args) != 2:
            raise ArityError(command, 2, len(args))
        elif command == "EXPIRE" and len(args) != 2:
            raise ArityError(command, 2, len(args))
        elif command == "PEXPIRE" and len(args) != 2:
            raise ArityError(command, 2, len(args))
        elif command == "TTL" and len(args) != 1:
            raise ArityError(command, 1, len(args))
        elif command == "PTTL" and len(args) != 1:
            raise ArityError(command, 1, len(args))
        elif command == "KEYS" and len(args) != 1:
            raise ArityError(command, 1, len(args))

        return self._commands[command](*args)

    def connection_handler(self, conn: Any, address: Any) -> None:
        socket_file = conn.makefile("rwb")

        try:
            while True:
                try:
                    data = self._protocol.handle_request(socket_file)
                except DisconnectError:
                    break

                try:
                    resp = self.get_response(data)
                except (
                    CommandError,
                    ArityError,
                    WrongTypeError,
                    AttributeError,
                ) as exc:
                    resp = Error(str(exc))

                try:
                    self._protocol.write_response(socket_file, resp)
                except OSError:
                    break
        finally:
            try:
                socket_file.close()
            except Exception:
                pass

    def run(self) -> None:
        try:
            self._server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            self._aof_manager.stop()

    def shutdown(self) -> None:
        
        self._aof_manager.stop()
        self._server.stop()


if __name__ == "__main__":
    monkey.patch_all()
    Server().run()
