from __future__ import annotations

import os
import threading
import time
from enum import Enum
from io import TextIOWrapper


class FsyncPolicy(Enum):
    ALWAYS = "always"  # fsync after every write
    EVERYSEC = "everysec"  # fsync every second
    NO = "no"  # never fsync (OS decides)


class AOFManager:
    def __init__(
        self,
        aof_file: str = "redis_clone.aof",
        fsync_policy: FsyncPolicy = FsyncPolicy.EVERYSEC,
    ):
        self.aof_file = aof_file
        self.fsync_policy = fsync_policy
        self._file: TextIOWrapper | None = None
        self._lock = threading.Lock()
        self._last_fsync = time.time()
        self._fsync_thread: threading.Thread | None = None
        self._stop_fsync = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self._file is None:
                # Open file in append mode
                self._file = open(self.aof_file, "a", encoding="utf-8")

                # Start background fsync thread for everysec policy
                if self.fsync_policy == FsyncPolicy.EVERYSEC:
                    self._stop_fsync.clear()
                    self._fsync_thread = threading.Thread(
                        target=self._fsync_worker, daemon=True
                    )
                    self._fsync_thread.start()

    def stop(self) -> None:
        with self._lock:
            if self._file is not None:
                # Stop background fsync thread
                if self._fsync_thread is not None:
                    self._stop_fsync.set()
                    self._fsync_thread.join(timeout=2.0)
                    self._fsync_thread = None

                # Final fsync and close
                if self.fsync_policy != FsyncPolicy.NO:
                    self._file.flush()
                    os.fsync(self._file.fileno())

                self._file.close()
                self._file = None

    def append_command(self, command: str, *args: str) -> None:
        if self._file is None:
            return

        # Format command as Redis protocol
        parts = [command] + list(args)
        aof_line = f"*{len(parts)}\r\n"

        for part in parts:
            aof_line += f"${len(part)}\r\n{part}\r\n"

        with self._lock:
            self._file.write(aof_line)
            self._file.flush()

            # Handle fsync based on policy
            if self.fsync_policy == FsyncPolicy.ALWAYS:
                os.fsync(self._file.fileno())
            elif self.fsync_policy == FsyncPolicy.EVERYSEC:
                # Background thread handles this
                pass
            # NO policy: let OS decide

    def _fsync_worker(self) -> None:
        while not self._stop_fsync.wait(1.0):  # Wait 1 second or until stop
            with self._lock:
                if self._file is not None:
                    self._file.flush()
                    os.fsync(self._file.fileno())
                    self._last_fsync = time.time()

    def replay_commands(self, command_handler: callable) -> int:
        if not os.path.exists(self.aof_file):
            return 0

        commands_replayed = 0
        last_valid_position = 0

        try:
            with open(self.aof_file, encoding="utf-8") as f:
                while True:
                    try:
                        # Read array length
                        line = f.readline()
                        if not line:
                            break

                        if not line.startswith("*"):
                            # Invalid format, truncate here
                            break

                        num_elements = int(line[1:].strip())

                        # Read command and arguments
                        command_parts = []
                        for _ in range(num_elements):
                            # Read string length
                            length_line = f.readline()
                            if not length_line.startswith("$"):
                                # Invalid format, truncate here
                                break

                            length = int(length_line[1:].strip())

                            # Read string data
                            data = f.read(length)
                            if len(data) != length:
                                # Incomplete data, truncate here
                                break

                            # Skip \r\n
                            f.read(2)

                            command_parts.append(data)

                        if len(command_parts) != num_elements:
                            # Incomplete command, truncate here
                            break

                        # Execute command
                        if command_parts:
                            command_handler(command_parts[0], *command_parts[1:])
                            commands_replayed += 1
                            last_valid_position = f.tell()

                    except (ValueError, IndexError, UnicodeDecodeError):
                        # Invalid command format, truncate at last valid position
                        break

        except OSError:
            # File read error, truncate at last valid position
            pass

        # Truncate file at last valid position if corruption detected
        if last_valid_position > 0:
            try:
                with open(self.aof_file, "r+", encoding="utf-8") as f:
                    f.seek(last_valid_position)
                    f.truncate()
            except OSError:
                pass

        return commands_replayed

    def get_file_size(self) -> int:
        try:
            return os.path.getsize(self.aof_file)
        except OSError:
            return 0

    def is_enabled(self) -> bool:
        return self._file is not None
