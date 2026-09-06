import asyncio
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("re")


def _host(proxy: str | None) -> str:
    return proxy.split("//")[1].split(":")[0] if proxy else ""


def _one_per_host(slots: list) -> list:
    seen = set()
    out = []
    for s in slots:
        h = _host(s.proxy)
        if h and h in seen:
            continue
        seen.add(h)
        out.append(s)
    return out


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
    captcha_streak: int = 0
    cooldown_until: float = 0
    ip: str = ""
    retired: bool = False
    _last_req: float = 0
    rate_limit: float = 3.0
    window_start: float = 0
    window_reqs: int = 0


class HttpPool:

    def __init__(self, slots: list[HttpSlot], rate_limit=3.0, max_concurrent_per_proxy=2,
                 burst=3, window=30.0, quarantine_cooldown=1800, waf_limit=8, budget=180):
        self._slots = _one_per_host(slots)
        self._idx = 0
        self._lock = asyncio.Lock()
        self._proxy_sems: dict[str, asyncio.Semaphore] = {}
        self._max_concurrent = max_concurrent_per_proxy
        self._burst = burst
        self._window = window
        self._quarantine_cooldown = quarantine_cooldown
        self._waf_limit = waf_limit
        self._rate_limit = rate_limit
        self._budget = budget
        self.retired_total = 0
        self.supply = None
        self.spent_hosts: set[str] = set()
        self.src_stats: dict[str, dict] = {}
        now = time.monotonic()
        for s in self._slots:
            s.rate_limit = rate_limit
            s.budget = budget
            s.window_start = now
            s.window_reqs = 0

    def _src(self, slot: HttpSlot) -> dict:
        return self.src_stats.setdefault(
            slot.source or "?",
            {"ok": 0, "net": 0, "waf": 0, "captcha": 0, "quarantined": 0, "retired": 0},
        )

    async def acquire(self) -> HttpSlot | None:
        while True:
            busy = False
            async with self._lock:
                slot, wait = self._next_available_rate_limited()
                if not slot:
                    return None
                if wait <= 0:
                    sem = self._get_proxy_sem(slot.proxy)
                    if sem.locked() and sem._value == 0:
                        busy = True
                    else:
                        slot._last_req = time.monotonic()
                        slot.reqs += 1
                        slot.window_reqs += 1
                        return slot
            await asyncio.sleep(0.05 if busy else min(wait, 0.5))

    async def acquire_concurrent(self, slot: HttpSlot):
        sem = self._get_proxy_sem(slot.proxy)
        await sem.acquire()

    def release_concurrent(self, slot: HttpSlot):
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
            if self._window > 0 and now - slot.window_start >= self._window:
                slot.window_start = now
                slot.window_reqs = 0
            min_interval = 1.0 / slot.rate_limit
            wait = slot._last_req + min_interval - now
            if self._window > 0 and slot.window_reqs >= self._burst:
                wait = max(wait, slot.window_start + self._window - now)
            if wait <= 0:
                return slot, 0
            if wait < best_wait:
                best = slot
                best_wait = wait
        return (best, best_wait) if best else (None, 0)

    def _park(self, slot: HttpSlot, cooldown: float):
        slot.cooldown_until = max(slot.cooldown_until, time.monotonic() + cooldown)

    def retire(self, slot: HttpSlot, reason: str):
        if slot.retired:
            return
        slot.retired = True
        self.spent_hosts.add(_host(slot.proxy))
        self._src(slot)["retired"] += 1
        self.retired_total += 1
        self._slots = [s for s in self._slots if s is not slot]
        log.debug(f"[HTTP] {slot.label} выработан ({reason}, {slot.reqs} req), убран из пула")

    def report_ok(self, slot: HttpSlot):
        slot.waf = 0
        slot.neterr = 0
        slot.dead_cycles = 0
        slot.captcha_streak = 0
        self._src(slot)["ok"] += 1
        if slot.budget and slot.reqs >= slot.budget:
            self.retire(slot, "budget")

    def report_waf(self, slot: HttpSlot, cooldown=30):
        slot.waf += 1
        self._park(slot, cooldown)
        slot.window_start = time.monotonic()
        slot.window_reqs = self._burst
        self._src(slot)["waf"] += 1
        if slot.waf >= self._waf_limit:
            self.retire(slot, "waf")

    def report_captcha(self, slot: HttpSlot, cooldown=2, streak_limit=6, streak_cooldown=1800):
        slot.captcha_streak += 1
        self._src(slot)["captcha"] += 1
        if streak_limit and slot.captcha_streak >= streak_limit:
            self.retire(slot, "captcha")
        elif cooldown:
            self._park(slot, cooldown)

    def report_net_error(self, slot: HttpSlot, threshold=3, cooldown=20, quarantine_after=8):
        slot.neterr += 1
        self._src(slot)["net"] += 1
        if slot.neterr >= threshold:
            slot.neterr = 0
            slot.dead_cycles += 1
            if quarantine_after and slot.dead_cycles >= quarantine_after:
                self._src(slot)["quarantined"] += 1
                self.retire(slot, "net")
            else:
                self._park(slot, cooldown)
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
        empty = {"ok": 0, "net": 0, "waf": 0, "captcha": 0, "quarantined": 0, "retired": 0}
        for k in sorted(set(list(total) + list(self.src_stats))):
            st = self.src_stats.get(k, empty)
            rows.append((k, alive.get(k, 0), total.get(k, 0), st["ok"], st["net"],
                         st["waf"], st["captcha"], st["quarantined"], st["retired"]))
        return rows

    def report_budget(self, slot: HttpSlot, cooldown=300):
        self.retire(slot, "budget")

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

    def debug_state(self) -> str:
        now = time.monotonic()
        reqs = sum(s.reqs for s in self._slots)
        cooling = sum(1 for s in self._slots if s.cooldown_until > now)
        avg = reqs / len(self._slots) if self._slots else 0
        sup = f" | supply {self.supply.stat()}" if self.supply else ""
        return f"avg={avg:.0f}/{self._budget} cool={cooling} spent={self.retired_total}{sup}"

    def add_slot(self, slot: HttpSlot) -> bool:
        h = _host(slot.proxy)
        if h and (h in self.spent_hosts or h in self.slot_hosts()):
            return False
        slot.budget = self._budget
        slot.rate_limit = self._rate_limit
        slot.window_start = time.monotonic()
        self._slots.append(slot)
        return True

    def remove_slot(self, label: str):
        self._slots = [s for s in self._slots if s.label != label]

    def slot_labels(self) -> set[str]:
        return {s.label for s in self._slots}

    def slot_hosts(self) -> set[str]:
        return {_host(s.proxy) for s in self._slots if s.proxy}

    def proxy_urls(self) -> set[str]:
        now = time.monotonic()
        return {s.proxy for s in self._slots if s.proxy and s.cooldown_until <= now}

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
