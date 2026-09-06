import asyncio
import logging
import queue
import threading
import time

from pipeline.cian.proxy_farm.pool import HttpPool, HttpSlot
from pipeline.cian.proxy_farm.sources import public_lists
from pipeline.cian.proxy_farm.validator import check_cian_live

log = logging.getLogger("re")


class ProxySupply:

    def __init__(self, pool: HttpPool, cfg: dict):
        self.pool = pool
        self.cfg = cfg
        self.target = cfg.get("proxy_target_slots", 120)
        self.timeout = cfg.get("proxy_validate_timeout", 20)
        self.connect_timeout = cfg.get("proxy_connect_timeout", 12)
        self.workers = cfg.get("validation_concurrency", 50)
        self.list_ttl = cfg.get("proxy_list_refresh", 900)
        self.recheck_after = cfg.get("proxy_recheck_after", 1500)
        self.tried: set[str] = set()
        self.alive: set[str] = set()
        self.ready: queue.Queue = queue.Queue()
        self.stopped = threading.Event()
        self.pending: asyncio.Queue | None = None
        self.checked = 0
        self.found = 0
        self.passes = 0
        self._listed_at = 0.0
        self._tried_at = 0.0

    def stat(self):
        q = self.pending.qsize() if self.pending else 0
        return f"проверено {self.checked} живых {self.found} очередь {q} круг {self.passes}"

    async def _refill(self):
        cands = await public_lists.fetch_candidates(self.cfg)
        self._listed_at = time.monotonic()
        if self.pending.qsize() < self.workers and time.monotonic() - self._tried_at > self.recheck_after:
            self.tried.clear()
            self._tried_at = time.monotonic()
            self.passes += 1
        spent = set(self.pool.spent_hosts)
        live = self.pool.slot_hosts()
        added = 0
        for p in cands:
            if p in self.tried:
                continue
            host = p.split("//")[1].split(":")[0]
            if host in spent or host in live:
                continue
            self.pending.put_nowait(p)
            added += 1
        log.info(f"[SUPPLY] +{added} кандидатов, проверено {self.checked}, живых {self.found}")

    async def _worker(self):
        while not self.stopped.is_set():
            if self.pool.alive >= self.target:
                await asyncio.sleep(5)
                continue
            try:
                proxy = self.pending.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(2)
                continue
            self.tried.add(proxy)
            self.checked += 1
            ms = await check_cian_live(
                proxy=proxy, timeout=self.timeout, connect_timeout=self.connect_timeout
            )
            if ms is not None:
                self.alive.add(proxy)
                self.ready.put(proxy)

    async def _keeper(self):
        while not self.stopped.is_set():
            await asyncio.sleep(20)
            if self.pending.qsize() < self.workers * 8 or time.monotonic() - self._listed_at > self.list_ttl:
                try:
                    await self._refill()
                except Exception as e:
                    log.warning(f"[SUPPLY] refill: {type(e).__name__}")
            spent = set(self.pool.spent_hosts)
            public_lists.save_cache(
                [p for p in self.alive if p.split("//")[1].split(":")[0] not in spent]
            )

    async def _run(self):
        self.pending = asyncio.Queue()
        self._tried_at = time.monotonic()
        await self._refill()
        await asyncio.gather(self._keeper(), *[self._worker() for _ in range(self.workers)])

    def start_thread(self):
        threading.Thread(target=lambda: asyncio.run(self._run()), daemon=True).start()

    async def drain(self):
        try:
            while True:
                while True:
                    try:
                        proxy = self.ready.get_nowait()
                    except queue.Empty:
                        break
                    if self.pool.add_slot(
                        HttpSlot(proxy=proxy, label=f"pub-{proxy.split('//')[1]}", source="public")
                    ):
                        self.found += 1
                await asyncio.sleep(1)
        finally:
            self.stopped.set()


async def run_refresher(pool: HttpPool, cfg: dict):
    supply = ProxySupply(pool, cfg)
    pool.supply = supply
    supply.start_thread()
    await supply.drain()
