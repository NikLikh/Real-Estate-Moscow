# proxy_farm/pool.py
import asyncio
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("re")


@dataclass
class HttpSlot:
    proxy: str | None
    label: str
    budget: int = 9000
    reqs: int = 0
    waf: int = 0
    cooldown_until: float = 0
    ip: str = ""
    _last_req: float = 0
    rate_limit: float = 3.0


class HttpPool:

    def __init__(self, slots: list[HttpSlot], rate_limit=3.0):
        self._slots = slots
        self._idx = 0
        self._lock = asyncio.Lock()
        for s in slots:
            s.rate_limit = rate_limit

    async def acquire(self) -> HttpSlot | None:
        while True:
            async with self._lock:
                slot, wait = self._next_available_rate_limited()
                if not slot:
                    return None
                if wait <= 0:
                    slot._last_req = time.monotonic()
                    slot.reqs += 1
                    return slot
            await asyncio.sleep(min(wait, 0.1))

    def _next_available_rate_limited(self) -> tuple:
        now = time.monotonic()
        n = len(self._slots)
        best = None
        best_wait = float("inf")
        for _ in range(n):
            slot = self._slots[self._idx % n]
            self._idx += 1
            if slot.cooldown_until > now:
                continue
            if slot.budget and slot.reqs >= slot.budget:
                continue
            min_interval = 1.0 / slot.rate_limit
            wait = slot._last_req + min_interval - now
            if wait <= 0:
                return slot, 0
            if wait < best_wait:
                best = slot
                best_wait = wait
        return best, best_wait if best else (None, 0)

    def report_ok(self, slot: HttpSlot):
        slot.waf = 0

    def report_waf(self, slot: HttpSlot, cooldown=30):
        slot.waf += 1
        slot.cooldown_until = time.monotonic() + cooldown
        log.debug(f"[HTTP] {slot.label} waf #{slot.waf}, cooling {cooldown}s")

    def report_budget(self, slot: HttpSlot, cooldown=300):
        slot.cooldown_until = time.monotonic() + cooldown
        log.info(
            f"[HTTP] {slot.label} budget exhausted ({slot.reqs}), cooling {cooldown}s"
        )
        slot.reqs = 0

    @property
    def alive(self) -> int:
        now = time.monotonic()
        return sum(
            1
            for s in self._slots
            if s.cooldown_until <= now and (not s.budget or s.reqs < s.budget)
        )

    @property
    def slot_count(self) -> int:
        return len(self._slots)

    def add_slot(self, slot: HttpSlot):
        slot.budget = self._slots[0].budget if self._slots else 9000
        slot.rate_limit = self._slots[0].rate_limit if self._slots else 3.0
        self._slots.append(slot)

    def remove_slot(self, label: str):
        self._slots = [s for s in self._slots if s.label != label]

    def slot_labels(self) -> set[str]:
        return {s.label for s in self._slots}

    def get_slot(self, label: str) -> HttpSlot | None:
        for s in self._slots:
            if s.label == label:
                return s
        return None

    def reset_cooldown(self, label: str):
        s = self.get_slot(label)
        if s:
            s.cooldown_until = 0
            s.waf = 0
