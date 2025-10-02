#!/usr/bin/env python3
"""Main entry point for the Redis Clone server."""

from gevent import monkey

from src.redis_clone.server import Server

if __name__ == "__main__":
    monkey.patch_all()
    Server().run()
