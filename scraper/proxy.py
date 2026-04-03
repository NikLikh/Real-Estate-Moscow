"""Multi-endpoint IP pool для cian.ru."""

import asyncio
import logging
import os
import socket
import subprocess
import time
from pathlib import Path

log = logging.getLogger("re")


class GotoThrottle:

    def __init__(self, max_per_sec=4.0):
        self._interval = 1.0 / max_per_sec
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            delay = self._last + self._interval - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = time.monotonic()


class _Endpoint:
    def __init__(self, name, proxy_str, rate_limit):
        self.name = name
        self.proxy_str = proxy_str
        self.rate_limit = rate_limit
        self.throttle = GotoThrottle(rate_limit)
        self.cooling_until = 0.0
        self.waf_count = 0

    @property
    def proxy(self):
        # playwright-compatible dict или None для direct
        if not self.proxy_str:
            return None
        return {"server": self.proxy_str}

    @property
    def active(self):
        return time.monotonic() >= self.cooling_until

    def to_dict(self):
        return {"name": self.name, "proxy": self.proxy, "throttle": self.throttle}


class ProxyPool:
    def __init__(self, cfg):
        raw = cfg.get("endpoints")
        if raw:
            self._endpoints = [
                _Endpoint(ep["name"], ep.get("proxy"), ep.get("rate_limit", 4.0))
                for ep in raw
            ]
        elif cfg.get("use_proxy") and cfg.get("proxies"):
            # обратная совместимость со старым форматом
            self._endpoints = [_Endpoint("direct", None, 4.0)]
            for i, p in enumerate(cfg["proxies"]):
                self._endpoints.append(
                    _Endpoint(f"proxy_{i}", p["server"], 4.0)
                )
        else:
            self._endpoints = [_Endpoint("direct", None, 4.0)]

        self._index = 0
        names = [ep.name for ep in self._endpoints]
        log.info(f"[POOL] {len(self._endpoints)} endpoints: {', '.join(names)}")

    def get_endpoint(self):
        # round-robin среди активных, fallback на любой если все cooling
        active = [ep for ep in self._endpoints if ep.active]
        if not active:
            log.warning("[POOL] all endpoints cooling, using fallback")
            active = self._endpoints

        ep = active[self._index % len(active)]
        self._index += 1
        return ep.to_dict()

    def report_waf(self, name, cooldown_sec=30):
        for ep in self._endpoints:
            if ep.name == name:
                ep.cooling_until = time.monotonic() + cooldown_sec
                ep.waf_count += 1
                log.info(f"[POOL] {name} cooling {cooldown_sec}s (waf #{ep.waf_count})")
                break

    def report_success(self, name):
        for ep in self._endpoints:
            if ep.name == name:
                ep.waf_count = 0
                break

    def get_healthy(self):
        return [ep.name for ep in self._endpoints if ep.active]

    def all_cooling(self):
        return all(not ep.active for ep in self._endpoints)

    @property
    def is_direct(self):
        return len(self._endpoints) == 1 and not self._endpoints[0].proxy_str

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass


def _port_open(host, port, timeout=2):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


async def _check_ip(pw, proxy_dict=None):
    args = ["--no-proxy-server"]  # не подхватывать системный прокси
    browser = await pw.chromium.launch(headless=True, args=args)
    try:
        ctx = await browser.new_context(proxy=proxy_dict) if proxy_dict else await browser.new_context()
        page = await ctx.new_page()
        await page.goto("https://httpbin.org/ip", timeout=10000)
        text = await page.inner_text("body")
        ip = text.split('"origin"')[1].split('"')[1].strip() if '"origin"' in text else None
        await page.close()
        await ctx.close()
        return ip
    except Exception as e:
        log.debug(f"check_ip failed: {e}")
        return None
    finally:
        await browser.close()


_vds_proc = None  # глобальная ссылка чтобы не убил GC


def ensure_vds_tunnel(cfg):
    global _vds_proc

    host = os.getenv("VDS_HOST", "")
    user = os.getenv("VDS_USER", "")
    if not host or not user:
        return

    port = int(cfg.get("vds_socks_port", 9080))

    # если порт уже слушает, туннель жив
    if _port_open("127.0.0.1", port):
        log.info(f"[VDS] tunnel already on :{port}")
        return

    # проверяем что VDS доступен
    if not _port_open(host, 22, timeout=5):
        log.info(f"[VDS] {host}:22 unreachable, skip")
        return

    # ищем SSH-ключ
    key_paths = [
        Path.home() / ".ssh" / "id_ed25519",
        Path("C:/Home/.ssh/id_ed25519"),
    ]
    key = next((p for p in key_paths if p.exists()), None)
    if not key:
        log.info("[VDS] no SSH key, skip (run: python -m tools.vds_tunnel setup)")
        return

    cmd = [
        "ssh", "-D", str(port), "-N",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ExitOnForwardFailure=yes",
        "-i", str(key),
        f"{user}@{host}",
    ]

    log.info(f"[VDS] starting tunnel to {host} on :{port}...")
    _vds_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    # ждём пока порт откроется
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _port_open("127.0.0.1", port):
            log.info(f"[VDS] tunnel ready on :{port}")
            return
        time.sleep(0.5)

    log.warning("[VDS] tunnel failed to start")
    try:
        _vds_proc.terminate()
    except Exception:
        pass
    _vds_proc = None


def stop_vds_tunnel():
    global _vds_proc
    if _vds_proc:
        log.info("[VDS] stopping tunnel")
        try:
            _vds_proc.terminate()
            _vds_proc.wait(timeout=5)
        except Exception:
            pass
        _vds_proc = None


async def auto_discover(cfg):
    from patchright.async_api import async_playwright

    candidates = [
        {"name": "direct", "proxy": None, "rate_limit": 4.0},
    ]

    # VLESS, проверяем SOCKS5 порт
    vless_port = int(cfg.get("vless_socks_port", 10808))
    if _port_open("127.0.0.1", vless_port):
        candidates.append({
            "name": "vless",
            "proxy": f"socks5://127.0.0.1:{vless_port}",
            "rate_limit": 4.0,
        })

    # VDS, проверяем SSH tunnel порт
    vds_port = int(cfg.get("vds_socks_port", 9080))
    if _port_open("127.0.0.1", vds_port):
        candidates.append({
            "name": "vds",
            "proxy": f"socks5://127.0.0.1:{vds_port}",
            "rate_limit": 3.0,
        })

    # определяем IP каждого кандидата
    log.info(f"[DISCOVER] checking {len(candidates)} candidates...")
    results = []
    seen_ips = set()

    async with async_playwright() as pw:
        for c in candidates:
            proxy_dict = {"server": c["proxy"]} if c["proxy"] else None
            ip = await _check_ip(pw, proxy_dict)
            if not ip:
                log.info(f"  {c['name']}: FAILED (skip)")
                continue

            if ip in seen_ips:
                log.info(f"  {c['name']}: {ip} (duplicate, skip)")
                continue

            seen_ips.add(ip)
            results.append(c)
            log.info(f"  {c['name']}: {ip} -- ok")

    if not results:
        log.warning("[DISCOVER] no endpoints found, fallback to direct")
        results = [{"name": "direct", "proxy": None, "rate_limit": 4.0}]

    log.info(f"[DISCOVER] {len(results)} unique endpoints")
    return results
