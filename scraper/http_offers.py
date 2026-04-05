import asyncio
import json
import logging
import random
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from scraper.browser import CAPTCHA_SIGNS, USER_AGENTS, VPN_SIGNS, WAF_RATE_LIMIT_SIGNS
from scraper.parsers import parse_offer_page

log = logging.getLogger("re")


@dataclass
class HttpSlot:
    proxy: str | None  # None = direct, "socks5://..." or "http://..."
    label: str
    budget: int = 9000
    reqs: int = 0
    waf: int = 0
    cooldown_until: float = 0
    ip: str = ""
    _last_req: float = 0  # monotonic timestamp последнего запроса
    rate_limit: float = 3.0  # max req/s на этот слот


class HttpPool:

    def __init__(self, slots: list[HttpSlot], rate_limit=3.0):
        self._slots = slots
        self._idx = 0
        self._lock = asyncio.Lock()
        # per-slot семафор для rate limiting: 1 запрос за раз на слот
        self._slot_locks = {id(s): asyncio.Lock() for s in slots}
        for s in slots:
            s.rate_limit = rate_limit

    async def acquire(self) -> HttpSlot | None:
        async with self._lock:
            slot = self._next_available()
            if not slot:
                return None

        # per-slot lock: только 1 worker на слот одновременно
        slot_lock = self._slot_locks[id(slot)]
        await slot_lock.acquire()
        try:
            min_interval = 1.0 / slot.rate_limit
            now = time.monotonic()
            wait = slot._last_req + min_interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            slot._last_req = time.monotonic()
        finally:
            slot_lock.release()
        return slot

    def _next_available(self) -> HttpSlot | None:
        now = time.monotonic()
        n = len(self._slots)
        for _ in range(n):
            slot = self._slots[self._idx % n]
            self._idx += 1
            if slot.cooldown_until > now:
                continue
            if slot.budget and slot.reqs >= slot.budget:
                continue
            return slot
        return None

    def report_ok(self, slot: HttpSlot):
        slot.waf = 0

    def report_waf(self, slot: HttpSlot, cooldown=30):
        slot.waf += 1
        slot.cooldown_until = time.monotonic() + cooldown
        log.info(f"[HTTP] {slot.label} waf #{slot.waf}, cooling {cooldown}s")

    def report_budget(self, slot: HttpSlot, cooldown=300):
        slot.cooldown_until = time.monotonic() + cooldown
        log.info(f"[HTTP] {slot.label} budget exhausted ({slot.reqs}), cooling {cooldown}s")
        slot.reqs = 0

    @property
    def alive(self) -> int:
        now = time.monotonic()
        return sum(
            1 for s in self._slots
            if s.cooldown_until <= now and (not s.budget or s.reqs < s.budget)
        )

    @property
    def slot_count(self) -> int:
        return len(self._slots)

    def add_slot(self, slot: HttpSlot):
        slot.budget = self._slots[0].budget if self._slots else 9000
        slot.rate_limit = self._slots[0].rate_limit if self._slots else 3.0
        self._slots.append(slot)
        self._slot_locks[id(slot)] = asyncio.Lock()


def _headers():
    return {
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.cian.ru/",
        "Cache-Control": "no-cache",
    }


def _is_waf(html: str, status: int) -> bool:
    if status in (403, 429, 503):
        return True
    low = html.lower()
    return any(sign in low for sign in WAF_RATE_LIMIT_SIGNS)


def _is_captcha(html: str, url: str = "") -> bool:
    url_low = url.lower()
    if "captcha" in url_low or "showcaptcha" in url_low:
        return True
    low = html.lower()
    return any(sign.lower() in low for sign in CAPTCHA_SIGNS)


def _is_vpn_block(html: str) -> bool:
    low = html.lower()
    return any(sign.lower() in low for sign in VPN_SIGNS)


async def fetch_offer(session, url, slot, pool, stats, cfg):
    try:
        resp = await session.get(
            url, headers=_headers(), timeout=cfg.get("http_timeout", 15),
            allow_redirects=True,
        )
    except Exception as e:
        log.debug(f"[HTTP] {slot.label} network error: {e}")
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        return None

    html = resp.text
    if len(html) > 5_000_000:
        return None
    slot.reqs += 1
    if slot.budget and slot.reqs >= slot.budget:
        pool.report_budget(slot, cfg.get("http_budget_cooldown", 300))

    if _is_waf(html, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 30))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if _is_captcha(html, str(resp.url)):
        pool.report_waf(slot, cfg.get("http_captcha_cooldown", 60))
        stats["captchas"] = stats.get("captchas", 0) + 1
        return None

    if _is_vpn_block(html):
        pool.report_waf(slot, 60)
        return None

    if resp.status_code != 200:
        return None

    pool.report_ok(slot)

    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.find(attrs={"data-name": "OfferTitleNew"})
    if title_el:
        t = title_el.get_text(strip=True).lower()
        if "комната" in t or "доля" in t:
            stats["skipped"] = stats.get("skipped", 0) + 1
            return "skipped"

    data, price_history = parse_offer_page(html)

    if not data.get("price"):
        stats["no_price"] = stats.get("no_price", 0) + 1
        return None

    data["url"] = url.split("?")[0]
    data["source"] = "cian"
    data["parsed_at"] = datetime.now().isoformat()

    rows = [data]
    cur_price = data.get("price")
    for entry in price_history:
        if entry["price"] == cur_price:
            continue
        hist = data.copy()
        hist["price"] = entry["price"]
        hist["publication_date"] = entry["date"]
        hist["source"] = "cian_history"
        rows.append(hist)

    timings = {"worker": slot.label, "fetch_dt": 0}
    return rows, timings


async def http_offer_worker(name, url_queue, retry_queue, row_queue, pool, stats, cfg, cookies=None):
    # curl_cffi: один session per proxy, impersonate Chrome TLS
    sessions = {}

    async def get_session(proxy):
        if proxy not in sessions:
            sessions[proxy] = AsyncSession(
                impersonate="chrome", proxy=proxy, max_clients=20,
            )
        return sessions[proxy]

    skip_urls = cfg.get("skip_url_parts", [])

    try:
        while True:
            try:
                url = await asyncio.wait_for(url_queue.get(), timeout=60)
            except asyncio.TimeoutError:
                break

            if url is None:
                break

            # фильтр до fetch -- экономим IP budget
            if any(part in url for part in skip_urls):
                stats["skipped"] = stats.get("skipped", 0) + 1
                continue

            slot = await pool.acquire()
            if not slot:
                # все слоты cooling, подождем
                await retry_queue.put(url)
                await asyncio.sleep(5)
                continue

            session = await get_session(slot.proxy)
            result = await fetch_offer(session, url, slot, pool, stats, cfg)

            if result is None:
                await retry_queue.put(url)
            elif result == "skipped":
                pass
            else:
                rows, timings = result
                timings["worker"] = name
                await row_queue.put((rows, timings))
                stats["parsed"] = stats.get("parsed", 0) + 1
    finally:
        for s in sessions.values():
            await s.close()


async def _proxy_refresher(pool, cfg, interval=300):
    # каждые N секунд подгружаем свежие прокси и добавляем новые слоты
    known_addrs = {s.proxy for s in pool._slots}
    while True:
        await asyncio.sleep(interval)
        try:
            fresh = await discover_free_proxies(
                timeout=4, max_candidates=2000, validation_timeout=30,
            )
            added = 0
            for proxy_url in fresh:
                if proxy_url not in known_addrs:
                    known_addrs.add(proxy_url)
                    proto = "socks5" if "socks5" in proxy_url else ("socks4" if "socks4" in proxy_url else "http")
                    pool.add_slot(HttpSlot(proxy=proxy_url, label=f"fresh-{proto}-{pool.slot_count}"))
                    added += 1
            if added:
                log.info(f"[HTTP] refresher: +{added} new proxies, total {pool.slot_count} slots")
        except Exception as e:
            log.debug(f"[HTTP] refresher error: {e}")


async def run_http_workers(n, url_queue, retry_queue, row_queue, pool, stats, cfg, cookies=None):
    refresh_interval = cfg.get("proxy_refresh_interval", 300)
    refresher = asyncio.create_task(_proxy_refresher(pool, cfg, interval=refresh_interval))

    tasks = [
        asyncio.create_task(
            http_offer_worker(f"H{i+1}", url_queue, retry_queue, row_queue, pool, stats, cfg, cookies=cookies)
        )
        for i in range(n)
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        refresher.cancel()


async def fetch_listing(session, url, slot, pool, stats, cfg):
    try:
        resp = await session.get(
            url, headers=_headers(), timeout=cfg.get("http_timeout", 15),
            allow_redirects=True,
        )
    except Exception as e:
        log.debug(f"[LHTTP] {slot.label} network error: {e}")
        return None

    html = resp.text
    if len(html) > 5_000_000:  # >5MB = что-то не то
        return None
    slot.reqs += 1

    if _is_waf(html, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 30))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if _is_captcha(html, str(resp.url)):
        pool.report_waf(slot, cfg.get("http_captcha_cooldown", 60))
        return None

    if resp.status_code != 200:
        return None

    pool.report_ok(slot)

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("article", attrs={"data-name": "CardComponent"})
    if not cards:
        return []

    skip_urls = cfg.get("skip_url_parts", [])
    skip_phrases = cfg.get("skip_phrases", [])
    urls = []
    for card in cards:
        link_area = card.find("div", attrs={"data-name": "LinkArea"})
        if not link_area:
            continue
        a = link_area.find("a", href=True)
        if not a:
            continue
        href = a["href"].split("?")[0]
        if any(part in href for part in skip_urls):
            continue
        text = card.get_text().lower()
        if any(phrase in text for phrase in skip_phrases):
            continue
        urls.append(href)

    return urls


async def http_listing_worker(name, filters, url_queue, seen, completed, pool, stats, cfg):
    sessions = {}

    async def get_session(proxy):
        if proxy not in sessions:
            sessions[proxy] = AsyncSession(impersonate="chrome", proxy=proxy, max_clients=10)
        return sessions[proxy]

    max_pages = cfg.get("max_pages", 54)
    max_cached = cfg.get("max_cached_pages", 3)
    min_new = cfg.get("min_new_first_page", 5)

    try:
        for fi, filt in enumerate(filters):
            label = filt["label"]
            log.info(f"[{name}] FILTER {fi+1}/{len(filters)}: {label}")
            consecutive_cached = 0
            pg = 1

            while pg <= max_pages:
                slot = await pool.acquire()
                if not slot:
                    await asyncio.sleep(5)
                    continue

                url = f"{filt['url']}&p={pg}"
                session = await get_session(slot.proxy)
                result = await fetch_listing(session, url, slot, pool, stats, cfg)

                if result is None:
                    # WAF -- retry этой же страницы с другим слотом
                    await asyncio.sleep(2)
                    continue

                if len(result) == 0:
                    break

                new_count = 0
                cached = 0
                for href in result:
                    if href in seen:
                        cached += 1
                        continue
                    seen.add(href)
                    await url_queue.put(href)
                    new_count += 1

                log.info(
                    f"[{name}] {label} p.{pg}: cards={len(result)} new={new_count} "
                    f"cached={cached} | queue={url_queue.qsize()}"
                )

                if pg == 1 and new_count < min_new and new_count + cached < 20:
                    break

                if new_count == 0:
                    consecutive_cached += 1
                    if consecutive_cached >= max_cached:
                        break
                else:
                    consecutive_cached = 0

                pg += 1

            completed.append(label)
            log.info(f"[{name}] DONE: {label}")
    finally:
        for s in sessions.values():
            await s.close()


async def run_http_listings(n, filters, url_queue, seen, completed, pool, stats, cfg):
    chunks = [filters[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            http_listing_worker(f"HL{i+1}", chunks[i], url_queue, seen, completed, pool, stats, cfg)
        )
        for i in range(n)
        if chunks[i]
    ]
    await asyncio.gather(*tasks)


async def steal_cookies(browser):
    # не нужен для curl_cffi (impersonate Chrome TLS), но оставлен для совместимости
    return None


def _port_open(host, port, timeout=2):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


async def _check_ip(proxy=None, timeout=5) -> str | None:
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            resp = await s.get("https://api.ipify.org", timeout=timeout)
            ip = resp.text.strip()
            if "." in ip:
                return ip
    except Exception:
        pass
    return None


PROXY_SOURCES = [
    # GitHub raw -- обновляются CI/CD пайплайнами, самые надежные
    ("socks5", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt"),
    ("http",   "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt"),
    ("http",   "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
    ("socks5", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt"),
    ("http",   "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt"),
    ("http",   "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"),
    ("socks5", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt"),
    ("socks4", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt"),
    ("http",   "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"),
    # API сервисы
    ("socks5", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all"),
    ("socks4", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=5000&country=all"),
    ("http",   "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all"),
    # spys.me
    ("socks5", "https://spys.me/socks.txt"),
    ("http",   "https://spys.me/proxy.txt"),
]


import re

_ADDR_RE = re.compile(r"^(\d{1,3}\.){3}\d{1,3}:\d{2,5}$")


def _parse_proxy_lines(text):
    result = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # убрать protocol prefix
        for prefix in ("socks5://", "socks4://", "http://", "https://"):
            if line.startswith(prefix):
                line = line[len(prefix):]
                break

        # первый токен до пробела = предполагаемый ip:port
        addr = line.split()[0]

        # строгая валидация: только ip:port, никакого мусора
        if _ADDR_RE.match(addr):
            result.append(addr)
    return result


async def _fetch_all_candidates() -> dict[str, str]:
    # addr -> proto
    candidates = {}

    async with AsyncSession(impersonate="chrome") as s:
        fetch_tasks = []
        for default_proto, url in PROXY_SOURCES:
            async def fetch_one(default_proto=default_proto, url=url):
                try:
                    resp = await s.get(url, timeout=8)
                    for line in resp.text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        proto = default_proto
                        for pfx in ("socks5://", "socks4://", "http://", "https://"):
                            if line.startswith(pfx):
                                proto = pfx.rstrip(":/")
                                line = line[len(pfx):]
                                break
                        addr = line.split()[0]
                        if _ADDR_RE.match(addr) and addr not in candidates:
                            candidates[addr] = proto
                except Exception:
                    pass
            fetch_tasks.append(fetch_one())

        try:
            await asyncio.wait_for(asyncio.gather(*fetch_tasks), timeout=15)
        except asyncio.TimeoutError:
            pass

    return candidates


async def _validate_batch(items, timeout=4, validation_timeout=30) -> list[str]:
    sem = asyncio.Semaphore(150)
    working = []

    async def check(addr, proto):
        proxy_url = f"{proto}://{addr}"
        async with sem:
            ip = await _check_ip(proxy=proxy_url, timeout=timeout)
            if ip:
                working.append(proxy_url)

    try:
        await asyncio.wait_for(
            asyncio.gather(*(check(a, p) for a, p in items)),
            timeout=validation_timeout,
        )
    except asyncio.TimeoutError:
        pass

    return working


async def discover_free_proxies(
    timeout=4, batch_size=2000, validation_timeout=30,
    min_target=10, max_rounds=5,
) -> list[str]:
    candidates = await _fetch_all_candidates()

    log.info(f"[HTTP] proxy sources: {len(candidates)} unique candidates from {len(PROXY_SOURCES)} sources")
    if not candidates:
        return []

    all_items = list(candidates.items())
    random.shuffle(all_items)

    working = []
    offset = 0

    for round_n in range(1, max_rounds + 1):
        batch = all_items[offset:offset + batch_size]
        if not batch:
            break
        offset += batch_size

        log.info(f"[HTTP] round {round_n}: validating {len(batch)} proxies...")
        found = await _validate_batch(batch, timeout=timeout, validation_timeout=validation_timeout)
        working.extend(found)
        log.info(f"[HTTP] round {round_n}: +{len(found)}, total {len(working)} working")

        if len(working) >= min_target:
            break

    log.info(f"[HTTP] free proxies: {len(working)}/{min(offset, len(all_items))} working")
    return working


async def build_http_pool(cfg) -> HttpPool:
    slots = []

    # direct -- всегда
    ip = await _check_ip(timeout=5)
    if ip:
        slots.append(HttpSlot(proxy=None, label="direct", ip=ip))
        log.info(f"[HTTP] direct: {ip}")
    else:
        slots.append(HttpSlot(proxy=None, label="direct"))
        log.warning("[HTTP] direct: ip check failed, added anyway")

    # VDS SSH tunnel
    vds_port = int(cfg.get("vds_socks_port", 9080))
    if _port_open("127.0.0.1", vds_port):
        proxy = f"socks5://127.0.0.1:{vds_port}"
        ip = await _check_ip(proxy=proxy, timeout=5)
        if ip:
            slots.append(HttpSlot(proxy=proxy, label="vds", ip=ip))
            log.info(f"[HTTP] vds: {ip}")

    # VLESS tunnel
    vless_port = int(cfg.get("vless_socks_port", 10808))
    if _port_open("127.0.0.1", vless_port):
        proxy = f"socks5://127.0.0.1:{vless_port}"
        ip = await _check_ip(proxy=proxy, timeout=5)
        if ip:
            slots.append(HttpSlot(proxy=proxy, label="vless", ip=ip))
            log.info(f"[HTTP] vless: {ip}")

    # CyberGhost -- статический server_list.json, БЕЗ browser launch (приоритет)
    try:
        from scraper.vpn_ext import download_extension
        download_extension("cyberghost")
    except Exception as e:
        log.debug(f"[HTTP] cyberghost download: {e}")
    cg_servers = await _discover_cyberghost_servers()
    for label, proxy_url, ip in cg_servers:
        slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip))
    if cg_servers:
        log.info(f"[HTTP] cyberghost: {len(cg_servers)} servers")

    # Browsec -- требует headed browser для PAC extraction
    try:
        browsec_servers = await _discover_browsec_servers(cfg)
        for label, proxy_url, ip in browsec_servers:
            slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip))
        if browsec_servers:
            log.info(f"[HTTP] browsec: {len(browsec_servers)} servers")
    except Exception as e:
        log.warning(f"[HTTP] browsec discovery failed (non-fatal): {e}")

    # free proxies
    if cfg.get("free_proxy_discovery"):
        try:
            free = await discover_free_proxies()
            for i, proxy_url in enumerate(free):
                proto = "socks5" if "socks5" in proxy_url else "http"
                slots.append(HttpSlot(proxy=proxy_url, label=f"free-{proto}-{i}"))
        except Exception as e:
            log.warning(f"[HTTP] free proxy discovery failed: {e}")

    budget = cfg.get("ip_budget", 9000)
    rate = cfg.get("http_rate_per_slot", 3.0)
    for s in slots:
        s.budget = budget

    log.info(f"[HTTP] pool ready: {len(slots)} slots, {rate} req/s per slot")
    return HttpPool(slots, rate_limit=rate)


async def _discover_cyberghost_servers() -> list[tuple[str, str, str]]:
    # CyberGhost хранит серверы в статическом JSON внутри CRX
    # формат nodes: {dnsname: "hostname:9002"} -> HTTPS proxy на порту 9002
    server_list = Path(__file__).resolve().parent.parent / "extensions" / "cyberghost" / "assets" / "server_list.json"
    if not server_list.exists():
        return []

    with open(server_list) as f:
        locations = json.load(f)

    all_nodes = []
    for loc in locations:
        for node in loc.get("nodes", []):
            if node.get("dnsname"):
                all_nodes.append((loc["name"], node["dnsname"]))

    if not all_nodes:
        return []

    log.info(f"[HTTP] cyberghost: validating {len(all_nodes)} nodes...")
    result = []
    sem = asyncio.Semaphore(10)
    seen_ips = set()

    async def check(country, dnsname):
        proxy = f"https://{dnsname}"
        async with sem:
            ip = await _check_ip(proxy=proxy, timeout=8)
            if ip and ip not in seen_ips:
                seen_ips.add(ip)
                result.append((f"cg-{country}", proxy, ip))

    await asyncio.gather(*(check(c, d) for c, d in all_nodes))
    log.info(f"[HTTP] cyberghost: {len(result)}/{len(all_nodes)} nodes working ({len(seen_ips)} unique IPs)")
    return result


async def _discover_browsec_servers(cfg) -> list[tuple[str, str, str]]:
    # запускаем browser с Browsec, извлекаем proxy-серверы из PAC, валидируем
    vpn_cfg = cfg.get("vpn_extensions", [])
    browsec_cfg = next((v for v in vpn_cfg if v.get("extension") == "browsec"), None)
    if not browsec_cfg:
        return []

    servers_to_try = browsec_cfg.get("servers", [])
    if not servers_to_try:
        return []

    log.info(f"[HTTP] extracting Browsec proxy servers...")

    try:
        from patchright.async_api import async_playwright
        from scraper.vpn_ext import launch_vpn_context
    except ImportError:
        return []

    result = []
    try:
        async with async_playwright() as pw:
            # подключаемся к первому серверу чтобы получить PAC данные
            ctx, bg = await launch_vpn_context(pw, "browsec", servers_to_try[0], headless=False)
            try:
                pac_data = await bg.evaluate("""async () => {
                    const items = await new Promise(r => chrome.storage.local.get('lowLevelPac', r));
                    return items['lowLevelPac'];
                }""")
            finally:
                await ctx.close()

        if not pac_data or "countries" not in pac_data:
            log.warning("[HTTP] browsec: no PAC data")
            return []

        # собираем все серверы из всех стран
        all_servers = []
        for country, servers in pac_data["countries"].items():
            for raw in servers:
                # формат: "HTTPS hostname:port"
                addr = raw.replace("HTTPS ", "").replace("HTTP ", "")
                all_servers.append((country, addr))

        log.info(f"[HTTP] browsec: {len(all_servers)} servers from {len(pac_data['countries'])} countries")

        # валидируем через curl_cffi
        sem = asyncio.Semaphore(10)

        async def check(country, addr):
            proxy = f"https://{addr}"
            async with sem:
                ip = await _check_ip(proxy=proxy, timeout=8)
                if ip:
                    result.append((f"vpn-{country}", proxy, ip))

        await asyncio.gather(*(check(c, a) for c, a in all_servers))
        log.info(f"[HTTP] browsec: {len(result)}/{len(all_servers)} servers working")

    except Exception as e:
        log.warning(f"[HTTP] browsec discovery failed: {e}")

    return result
