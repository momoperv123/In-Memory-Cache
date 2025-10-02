# Redis Clone

A Python implementation of Redis-like in-memory data store with TTL support, built with gevent.

## Features

- **String Operations**: `GET`, `SET`, `MGET`, `MSET`
- **Key Management**: `DELETE`, `EXISTS`, `KEYS`, `FLUSH`
- **TTL Operations**: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`
- **High Performance**: Gevent-based async networking
- **Redis-compatible**: Error messages and protocol

## Quick Start

### 1. Install Dependencies
```bash
pip install gevent pytest
```

### 2. Run the Server
```bash
python main.py
```
Server starts on `127.0.0.1:31337`

### 3. Use the Client
```python
import sys
sys.path.insert(0, 'src')
from redis_clone import Client

client = Client()
client.set('name', 'Alice')
print(client.get('name'))  # Alice

# TTL operations
client.execute('EXPIRE', 'name', 60)
print(client.execute('TTL', 'name'))  # remaining seconds
```

## Testing

**Important**: Start the server first in one terminal:
```bash
python main.py
```

Then run all tests in another terminal:
```bash
python -m pytest tests/ -v
```

## Project Structure

```
redis-clone/
├── src/redis_clone/     # Main package
├── tests/               # Test suite
├── main.py             # Server entry point
└── pyproject.toml      # Project config
```

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `GET key` | Get value | `GET name` |
| `SET key value` | Set value | `SET name Alice` |
| `EXPIRE key seconds` | Set TTL | `EXPIRE name 60` |
| `TTL key` | Get TTL | `TTL name` |
| `KEYS *` | List all keys | `KEYS *` |
| `FLUSH` | Delete all keys | `FLUSH` |

## Development

```bash
# Format code
python -m ruff format .

# Check issues
python -m ruff check --fix .
```