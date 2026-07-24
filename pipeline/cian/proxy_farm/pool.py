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
    source: str = ""
    budget: int = 9000
    reqs: int = 0
    waf: int = 0
    neterr: int = 0
    dead_cycles: int = 0
    cooldown_until: float = 0
    ip: str = ""
    _last_req: float = 0
    rate_limit: float = 3.0


class HttpPool:

    def __init__(self, slots: list[HttpSlot], rate_limit=3.0, max_concurrent_per_proxy=2):
        self._slots = slots
        self._idx = 0
        self._lock = asyncio.Lock()
        self._proxy_sems: dict[str, asyncio.Semaphore] = {}
        self._max_concurrent = max_concurrent_per_proxy
        self.src_stats: dict[str, dict] = {}
        for s in slots:
            s.rate_limit = rate_limit

    def _src(self, slot: HttpSlot) -> dict:
        return self.src_stats.setdefault(
            slot.source or "?", {"ok": 0, "net": 0, "waf": 0, "quarantined": 0}
        )

    async def acquire(self) -> HttpSlot | None:
        """выдаём слот с учётом rate limit И concurrent per proxy"""
        while True:
            async with self._lock:
                slot, wait = self._next_available_rate_limited()
                if not slot:
                    return None
                if wait <= 0:
                    slot._last_req = time.monotonic()
                    slot.reqs += 1
                    # проверяем per-proxy concurrency
                    sem = self._get_proxy_sem(slot.proxy)
                    if sem.locked() and sem._value == 0:
                        # все concurrent-слоты для этого прокси заняты, пробуем другой
                        continue
                    return slot
            await asyncio.sleep(min(wait, 0.1))

    async def acquire_concurrent(self, slot: HttpSlot):
        """захватываем concurrent-слот перед HTTP запросом"""
        sem = self._get_proxy_sem(slot.proxy)
        await sem.acquire()

    def release_concurrent(self, slot: HttpSlot):
        """освобождаем concurrent-слот после HTTP запроса"""
        sem = self._get_proxy_sem(slot.proxy)
        sem.release()

    def _get_proxy_sem(self, proxy: str) -> asyncio.Semaphore:
        if proxy not in self._proxy_sems:
            self._proxy_sems[proxy] = asyncio.Semaphore(self._max_concurrent)
        return self._proxy_sems[proxy]

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
        return (best, best_wait) if best else (None, 0)

    def report_ok(self, slot: HttpSlot):
        slot.waf = 0
        slot.neterr = 0
        slot.dead_cycles = 0
        self._src(slot)["ok"] += 1

    def report_waf(self, slot: HttpSlot, cooldown=30):
        slot.waf += 1
        slot.cooldown_until = time.monotonic() + cooldown
        self._src(slot)["waf"] += 1
        log.debug(f"[HTTP] {slot.label} waf #{slot.waf}, cooling {cooldown}s")

    def report_net_error(self, slot: HttpSlot, threshold=3, cooldown=20, quarantine_after=5):
        slot.neterr += 1
        self._src(slot)["net"] += 1
        if slot.neterr >= threshold:
            slot.neterr = 0
            slot.dead_cycles += 1
            if quarantine_after and slot.dead_cycles >= quarantine_after:
                slot.cooldown_until = time.monotonic() + 86400
                self._src(slot)["quarantined"] += 1
                log.info(f"[HTTP] {slot.label} мёртв ({slot.dead_cycles} циклов без успеха), убран из ротации")
            else:
                slot.cooldown_until = time.monotonic() + cooldown
                log.debug(f"[HTTP] {slot.label} {threshold} net errors подряд, cooling {cooldown}s")

    def source_breakdown(self) -> list[tuple]:
        now = time.monotonic()
        total = {}
        alive = {}
        for s in self._slots:
            k = s.source or "?"
            total[k] = total.get(k, 0) + 1
            if s.cooldown_until <= now and (not s.budget or s.reqs < s.budget):
                alive[k] = alive.get(k, 0) + 1
        rows = []
        for k in sorted(set(list(total) + list(self.src_stats))):
            st = self.src_stats.get(k, {"ok": 0, "net": 0, "waf": 0, "quarantined": 0})
            rows.append((k, alive.get(k, 0), total.get(k, 0), st["ok"], st["net"], st["waf"], st["quarantined"]))
        return rows

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
