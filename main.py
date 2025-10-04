from gevent import monkey

from src.redis_clone.server import Server

if __name__ == "__main__":
    monkey.patch_all()
    Server().run()
