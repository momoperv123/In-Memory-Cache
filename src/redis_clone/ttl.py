from __future__ import annotations

import heapq
import time


class TTLManager:
    def __init__(self) -> None:
        self.expiry_heap: list[tuple[float, str]] = []
        self.key_expiry: dict[str, float] = {}
        self.last_cleanup: float = time.time()
        self.cleanup_interval: float = 0.1

    def set_expiry(self, key: str, ttl_ms: int) -> bool:
        if ttl_ms < 0:
            return False

        expiry_time = time.time() * 1000 + ttl_ms
        self.key_expiry[key] = expiry_time
        heapq.heappush(self.expiry_heap, (expiry_time, key))
        return True

    def get_ttl(self, key: str) -> int:
        if key not in self.key_expiry:
            return -1  # Key exists but has no TTL set

        remaining_ms = self.key_expiry[key] - (time.time() * 1000)
        if remaining_ms <= 0:
            return -1  # TTL expired
        return int(remaining_ms)

    def is_expired(self, key: str) -> bool:
        if key not in self.key_expiry:
            return False
        return self.key_expiry[key] <= time.time() * 1000

    def remove_ttl(self, key: str) -> bool:
        if key not in self.key_expiry:
            return False
        del self.key_expiry[key]
        return True

    def cleanup_expired(self, force=False) -> set[str]:
        current_time = time.time()

        if not force and current_time - self.last_cleanup < self.cleanup_interval:
            return set()

        self.last_cleanup = current_time
        current_time_ms = current_time * 1000
        expired_keys = set()

        while self.expiry_heap and self.expiry_heap[0][0] <= current_time_ms:
            expiry_time, key = heapq.heappop(self.expiry_heap)

            if key in self.key_expiry and self.key_expiry[key] == expiry_time:
                expired_keys.add(key)
                del self.key_expiry[key]

        return expired_keys

    def clear(self) -> None:
        self.expiry_heap.clear()
        self.key_expiry.clear()
