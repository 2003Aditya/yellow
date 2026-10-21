import time
from collections import OrderedDict
from threading import Lock


class MetadataCache:
    """In-process, size-bounded TTL cache for *immutable-ish* coupon metadata.

    Entries are keyed by coupon code and carry the row ``version``; readers must check the
    version against the database, so a stale entry is never served (it is just a miss).
    Counters (redeemed_count / remaining) are never stored here.
    """

    def __init__(self, ttl_s: float, max_entries: int):
        self._ttl = ttl_s
        self._max = max_entries
        self._data: OrderedDict[str, tuple[float, int, dict]] = OrderedDict()
        self._lock = Lock()
        self.hits = 0
        self.misses = 0

    def get(self, code: str, version: int) -> dict | None:
        with self._lock:
            entry = self._data.get(code)
            if entry is None or entry[0] < time.monotonic() or entry[1] != version:
                self.misses += 1
                return None
            self._data.move_to_end(code)
            self.hits += 1
            return entry[2]

    def put(self, code: str, version: int, meta: dict) -> None:
        with self._lock:
            self._data[code] = (time.monotonic() + self._ttl, version, meta)
            self._data.move_to_end(code)
            while len(self._data) > self._max:
                self._data.popitem(last=False)

    def invalidate(self, code: str) -> None:
        with self._lock:
            self._data.pop(code, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
