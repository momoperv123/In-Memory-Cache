class RedisError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class WrongTypeError(RedisError):
    def __init__(self, operation: str, key: str, expected_type: str, actual_type: str):
        message = f"WRONGTYPE Operation {operation} against a {actual_type} holding the wrong kind of value"
        super().__init__(message)
        self.operation = operation
        self.key = key
        self.expected_type = expected_type
        self.actual_type = actual_type


class ArityError(RedisError):
    def __init__(self, command: str, expected_args: int, actual_args: int):
        message = f"ERR wrong number of arguments for '{command}' command"
        super().__init__(message)
        self.command = command
        self.expected_args = expected_args
        self.actual_args = actual_args


class CommandError(RedisError):
    def __init__(self, command: str):
        message = f"ERR unknown command {command}"
        super().__init__(message)
        self.command = command
