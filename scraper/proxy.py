"""Runtime endpoint discovery, preflight, proxy pool."""

import asyncio
import logging
import os
import socket
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

from scraper.browser import (
    WebProxyPageAdapter,
    apply_cdp_blocking,
    create_stealth_context,
    detect_captcha,
    detect_vpn_block,
    detect_waf_rate_limit,
    launch_stealth_browser,
    warmup_session,
)
from scraper.runtime import EndpointDriver, EndpointKind

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


def _infer_slot_class(proxy_str, ep_type="proxy", driver="native"):
    if ep_type == EndpointKind.BROWSER_GATEWAY.value or driver != EndpointDriver.NATIVE.value:
        return "browser_gateway"
    if not proxy_str:
        return "direct"

    proxy_l = proxy_str.lower()
    if proxy_l.startswith("socks5://"):
        return "socks5"
    if proxy_l.startswith("http://") or proxy_l.startswith("https://"):
        return "http_proxy"
    return ep_type or "proxy"


def _normalize_endpoint(ep):
    proxy = ep.get("proxy")
    if isinstance(proxy, dict):
        proxy = proxy.get("server")

    kind = ep.get("kind") or ep.get("type", EndpointKind.PROXY.value)
    driver = ep.get("driver", EndpointDriver.NATIVE.value)
    slot_class = ep.get("slot_class") or _infer_slot_class(proxy, kind, driver)
    policy = dict(ep.get("policy") or {})
    rate_limit = ep.get("rate_limit", policy.get("rate_limit", 4.0))
    if not policy:
        policy = {"rate_limit": rate_limit}
    return {
        "name": ep["name"],
        "proxy": proxy,
        "rate_limit": rate_limit,
        "type": kind,
        "kind": kind,
        "driver": driver,
        "slot_class": slot_class,
        "ip": ep.get("ip"),
        "network_id": ep.get("network_id") or ep.get("ip"),
        "policy": policy,
        "vpn_cfg": ep.get("vpn_cfg"),
        "web_proxy_cfg": ep.get("web_proxy_cfg"),
        "runtime_enabled": ep.get("runtime_enabled", True),
        "experimental": ep.get("experimental", False),
        "last_verify_status": ep.get("last_verify_status"),
        "last_verify_reason": ep.get("last_verify_reason"),
    }


class _Endpoint:
    def __init__(self, raw, budget=9000):
        ep = _normalize_endpoint(raw)
        self.name = ep["name"]
        self.proxy_str = ep["proxy"]
        self.rate_limit = ep["rate_limit"]
        self.type = ep["type"]
        self.slot_class = ep["slot_class"]
        self.ip = ep.get("ip")
        self.last_verify_status = ep.get("last_verify_status", "unknown")
        self.last_verify_reason = ep.get("last_verify_reason", "")
        self.throttle = GotoThrottle(self.rate_limit)
        self.cooling_until = 0.0
        self.waf_count = 0
        self.req_count = 0
        self.budget = budget

    @property
    def proxy(self):
        if not self.proxy_str:
            return None
        return {"server": self.proxy_str}

    @property
    def active(self):
        return time.monotonic() >= self.cooling_until

    @property
    def budget_exhausted(self):
        return self.budget > 0 and self.req_count >= self.budget

    @property
    def available(self):
        return self.active and not self.budget_exhausted

    def to_dict(self):
        return {
            "name": self.name,
            "proxy": self.proxy,
            "throttle": self.throttle,
            "type": self.type,
            "slot_class": self.slot_class,
            "ip": self.ip,
            "last_verify_status": self.last_verify_status,
            "last_verify_reason": self.last_verify_reason,
        }


class ProxyPool:
    def __init__(self, cfg):
        budget = cfg.get("ip_budget", 9000)
        budget_cooldown = cfg.get("ip_budget_cooldown", 300)
        raw = cfg.get("verified_endpoints") or cfg.get("endpoints")
        raw = raw or [{"name": "direct", "proxy": None, "rate_limit": 4.0, "type": "proxy"}]

        self._endpoints = [_Endpoint(ep, budget=budget) for ep in raw]
        self._group_indexes = {}
        self._budget_cooldown = budget_cooldown

        names = [f"{ep.name}[{ep.slot_class}]" for ep in self._endpoints]
        log.info(f"[POOL] {len(self._endpoints)} verified endpoints: {', '.join(names)}")

        self._waf_times = []
        self._cb_window = cfg.get("cb_window", 30)
        self._cb_threshold = cfg.get("cb_threshold", 4)
        self._cb_cooldown = cfg.get("cb_cooldown", 120)
        self._cb_until = 0.0
        self._cb_tripped = False

    def _pick(self, candidates, key):
        if not candidates:
            return None
        idx = self._group_indexes.get(key, 0)
        ep = candidates[idx % len(candidates)]
        self._group_indexes[key] = idx + 1
        return ep

    def get_endpoint(self, prefer_name=None, allowed_names=None):
        allowed = set(allowed_names or [])

        if prefer_name:
            for ep in self._endpoints:
                if ep.name != prefer_name:
                    continue
                if allowed and ep.name not in allowed:
                    continue
                if ep.available:
                    return ep.to_dict()

        candidates = [
            ep for ep in self._endpoints
            if ep.available and (not allowed or ep.name in allowed)
        ]
        key = tuple(sorted(allowed)) if allowed else "all"
        ep = self._pick(candidates, key)
        if ep:
            return ep.to_dict()

        active = [
            ep for ep in self._endpoints
            if ep.active and (not allowed or ep.name in allowed)
        ]
        ep = self._pick(active, f"{key}:active")
        if ep:
            log.warning(f"[POOL] all allowed endpoints cooling, using active fallback: {ep.name}")
            return ep.to_dict()

        fallback_pool = [
            ep for ep in self._endpoints
            if not allowed or ep.name in allowed
        ]
        ep = self._pick(fallback_pool, f"{key}:all")
        if ep:
            if allowed:
                log.warning(
                    f"[POOL] all allowed endpoints cooling, using same-slot fallback: {ep.name}"
                )
            else:
                log.warning(f"[POOL] all endpoints cooling, using hard fallback: {ep.name}")
            return ep.to_dict()

        raise RuntimeError("proxy pool is empty")

    def report_request(self, name):
        for ep in self._endpoints:
            if ep.name != name:
                continue
            ep.req_count += 1
            if ep.budget_exhausted:
                ep.cooling_until = time.monotonic() + self._budget_cooldown
                log.info(
                    f"[POOL] {name} budget exhausted "
                    f"({ep.req_count}/{ep.budget}), cooling {self._budget_cooldown}s"
                )
                ep.req_count = 0
            break

    def report_waf(self, name, cooldown_sec=30):
        for ep in self._endpoints:
            if ep.name != name:
                continue
            ep.cooling_until = time.monotonic() + cooldown_sec
            ep.waf_count += 1
            log.info(f"[POOL] {name} cooling {cooldown_sec}s (waf #{ep.waf_count})")
            break

        now = time.monotonic()
        self._waf_times = [t for t in self._waf_times if now - t < self._cb_window]
        self._waf_times.append(now)
        if len(self._waf_times) >= self._cb_threshold:
            self._cb_until = time.monotonic() + self._cb_cooldown
            self._cb_tripped = True
            self._waf_times.clear()
            log.warning(f"[CIRCUIT] OPEN -- all workers paused for {self._cb_cooldown}s")

    def report_success(self, name):
        for ep in self._endpoints:
            if ep.name != name:
                continue
            ep.waf_count = 0
            break

    async def wait_if_open(self):
        if time.monotonic() >= self._cb_until:
            return False
        remaining = self._cb_until - time.monotonic()
        if remaining > 0:
            log.info(f"[CIRCUIT] waiting {remaining:.0f}s...")
            await asyncio.sleep(remaining)
        log.info("[CIRCUIT] CLOSED -- resuming")
        return True

    @property
    def was_tripped(self):
        return self._cb_tripped

    def ack_trip(self):
        self._cb_tripped = False

    def get_healthy(self):
        return [ep.name for ep in self._endpoints if ep.active]

    def get_runtime_endpoints(self):
        return [ep.to_dict() for ep in self._endpoints]

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


async def _resolve_public_ip(ctx, timeout=10000):
    page = await ctx.new_page()
    try:
        checks = [
            ("https://api.ipify.org", lambda text: text.strip() if "." in text else None),
            ("https://ifconfig.me/ip", lambda text: text.strip() if "." in text else None),
            (
                "https://httpbin.org/ip",
                lambda text: text.split('"origin"')[1].split('"')[1].strip()
                if '"origin"' in text else None,
            ),
        ]
        for url, parser in checks:
            try:
                await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
                text = await page.inner_text("body")
                ip = parser(text)
                if ip:
                    return ip
            except Exception:
                continue
    finally:
        try:
            await page.close()
        except Exception:
            pass
    return None


async def _verify_loaded_page(page, started):
    title = await page.title()
    title_l = title.lower()
    url = page.url.lower()
    if await detect_vpn_block(page):
        return False, "vpn_block", title[:60], time.monotonic() - started
    if await detect_waf_rate_limit(page):
        return False, "waf_block", title[:60], time.monotonic() - started

    ok = (
        "cian" in title_l or
        "циан" in title_l or
        "недвижим" in title_l or
        "cian.ru" in url
    )
    if await detect_captcha(page):
        if ok or "cian.ru" in url:
            return True, "captcha_warmup", title[:60], time.monotonic() - started
        return False, "captcha", title[:60], time.monotonic() - started
    if not ok:
        return False, f"unexpected_title:{title[:60]}", title[:60], time.monotonic() - started
    return True, "ok", title[:60], time.monotonic() - started


async def _verify_cian_access(ctx):
    page = await ctx.new_page()
    cdp = None
    try:
        cdp = await apply_cdp_blocking(page)
        started = time.monotonic()
        await warmup_session(page)
        return await _verify_loaded_page(page, started)
    except Exception as e:
        return False, f"warmup_failed:{str(e)[:60]}", None, 0.0
    finally:
        if cdp:
            try:
                await cdp.detach()
            except Exception:
                pass
        try:
            await page.close()
        except Exception:
            pass


async def _preflight_native_candidate(pw, candidate, retries=2):
    proxy_dict = {"server": candidate["proxy"]} if candidate.get("proxy") else None
    last_reason = "unknown"
    last_title = None
    last_latency = 0.0

    for _ in range(retries):
        browser = await launch_stealth_browser(pw, headless=True)
        ctx = None
        try:
            ctx = await create_stealth_context(browser, proxy=proxy_dict)
            ip = await _resolve_public_ip(ctx)
            if not ip:
                last_reason = "ip_check_failed"
                continue

            ok, reason, title, latency = await _verify_cian_access(ctx)
            last_reason = reason
            last_title = title
            last_latency = latency
            if ok:
                return {
                    "ok": True,
                    "network_id": ip,
                    "reason": reason,
                    "title": title,
                    "latency": latency,
                }
        finally:
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
            await browser.close()

        await asyncio.sleep(1)

    return {
        "ok": False,
        "network_id": None,
        "reason": last_reason,
        "title": last_title,
        "latency": last_latency,
    }


async def _preflight_vpn_candidate(pw, candidate, retries=2):
    from scraper.vpn_ext import check_vpn_ip, launch_vpn_context

    vpn = candidate["vpn_cfg"]
    headless = vpn.get("headless", False)
    last_reason = "unknown"
    last_title = None
    last_latency = 0.0

    for _ in range(retries):
        ctx = None
        try:
            ctx, _ = await launch_vpn_context(
                pw,
                vpn["extension"],
                vpn["server"],
                headless=headless,
                cfg_path=vpn.get("path"),
            )
            network_id = await check_vpn_ip(ctx)
            if not network_id:
                last_reason = "ip_check_failed"
                continue
            ok, reason, title, latency = await _verify_cian_access(ctx)
            last_reason = reason
            last_title = title
            last_latency = latency
            if ok:
                return {
                    "ok": True,
                    "network_id": network_id,
                    "reason": reason,
                    "title": title,
                    "latency": latency,
                }
        finally:
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
        await asyncio.sleep(1)

    return {
        "ok": False,
        "network_id": None,
        "reason": last_reason,
        "title": last_title,
        "latency": last_latency,
    }


async def _preflight_web_proxy_candidate(pw, candidate, retries=2):
    last_reason = "unknown"
    last_title = None
    last_latency = 0.0
    network_id = candidate.get("network_id")

    for _ in range(retries):
        browser = await launch_stealth_browser(pw, headless=True)
        ctx = None
        try:
            ctx = await create_stealth_context(browser)
            raw_page = await ctx.new_page()
            await apply_cdp_blocking(raw_page)
            adapter = WebProxyPageAdapter(raw_page, candidate["web_proxy_cfg"])
            started = time.monotonic()
            await adapter.goto(candidate["web_proxy_cfg"]["target_url"])
            ok, reason, title, latency = await _verify_loaded_page(adapter, started)
            last_reason = reason
            last_title = title
            last_latency = latency
            if ok:
                return {
                    "ok": True,
                    "network_id": network_id,
                    "reason": reason,
                    "title": title,
                    "latency": latency,
                }
        finally:
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
            await browser.close()
        await asyncio.sleep(1)

    return {
        "ok": False,
        "network_id": network_id,
        "reason": last_reason,
        "title": last_title,
        "latency": last_latency,
    }


async def _preflight_candidate(pw, candidate, retries=2):
    driver = candidate.get("driver", EndpointDriver.NATIVE.value)
    if driver == EndpointDriver.VPN_EXTENSION.value:
        return await _preflight_vpn_candidate(pw, candidate, retries=retries)
    if driver == EndpointDriver.WEB_PROXY.value:
        return await _preflight_web_proxy_candidate(pw, candidate, retries=retries)
    return await _preflight_native_candidate(pw, candidate, retries=retries)


def _endpoint_policy(cfg, rate_limit, overrides=None):
    runtime_cfg = cfg.get("endpoint_policies") or {}
    default_cfg = runtime_cfg.get("default") or {}
    overrides = overrides or {}
    policy = {
        "rate_limit": overrides.get("rate_limit", rate_limit),
        "budget_limit": overrides.get("budget_limit", cfg.get("ip_budget", 9000)),
        "budget_cooldown": overrides.get("budget_cooldown", cfg.get("ip_budget_cooldown", 300)),
        "waf_cooldown": overrides.get("waf_cooldown", cfg.get("waf_endpoint_cooldown", 30)),
        "network_cooldown": overrides.get("network_cooldown", default_cfg.get("network_cooldown", 20)),
        "captcha_cooldown": overrides.get("captcha_cooldown", default_cfg.get("captcha_cooldown", 30)),
        "quarantine_cooldown": overrides.get("quarantine_cooldown", default_cfg.get("quarantine_cooldown", 900)),
        "max_warming_failures": overrides.get(
            "max_warming_failures", default_cfg.get("max_warming_failures", 2)
        ),
        "max_quarantine_failures": overrides.get(
            "max_quarantine_failures", default_cfg.get("max_quarantine_failures", 3)
        ),
        "preflight_retries": overrides.get(
            "preflight_retries", cfg.get("endpoint_preflight_retries", 2)
        ),
    }
    return policy


def _runtime_endpoint_types(cfg):
    raw = set(cfg.get("runtime_endpoint_types", ["proxy"]))
    kinds = set()
    for item in raw:
        if item == EndpointKind.BROWSER_GATEWAY.value:
            kinds.add(EndpointKind.BROWSER_GATEWAY.value)
        elif item == EndpointKind.DIRECT.value:
            kinds.add(EndpointKind.DIRECT.value)
        else:
            kinds.add(EndpointKind.PROXY.value)
    if not kinds:
        kinds.add(EndpointKind.PROXY.value)
    return kinds


def _experimental_drivers(cfg):
    return set(cfg.get("experimental_endpoint_types", []))


def _collect_experimental_names(cfg):
    enabled = _experimental_drivers(cfg)
    names = []

    if EndpointDriver.VPN_EXTENSION.value in enabled:
        for vs in cfg.get("vpn_extensions", []):
            ext = vs["extension"]
            for server in vs.get("servers", []):
                names.append(f"vpn-{ext}-{server}")

    if EndpointDriver.WEB_PROXY.value in enabled:
        for candidate in cfg.get("web_proxy_candidates", []):
            names.append(candidate["name"])

    return names


def _discover_local_proxy_candidates(cfg):
    runtime_types = _runtime_endpoint_types(cfg)
    candidates = []

    if EndpointKind.PROXY.value in runtime_types or EndpointKind.DIRECT.value in runtime_types:
        candidates.append(_normalize_endpoint({
            "name": "direct",
            "proxy": None,
            "rate_limit": 4.0,
            "type": EndpointKind.DIRECT.value,
            "kind": EndpointKind.DIRECT.value,
            "driver": EndpointDriver.NATIVE.value,
            "slot_class": "direct",
            "policy": _endpoint_policy(cfg, 4.0),
        }))

    vless_port = int(cfg.get("vless_socks_port", 10808))
    if EndpointKind.PROXY.value in runtime_types and _port_open("127.0.0.1", vless_port):
        candidates.append(_normalize_endpoint({
            "name": "vless",
            "proxy": f"socks5://127.0.0.1:{vless_port}",
            "rate_limit": 4.0,
            "type": EndpointKind.PROXY.value,
            "kind": EndpointKind.PROXY.value,
            "driver": EndpointDriver.NATIVE.value,
            "slot_class": "socks5",
            "policy": _endpoint_policy(cfg, 4.0),
        }))

    vds_port = int(cfg.get("vds_socks_port", 9080))
    if EndpointKind.PROXY.value in runtime_types and _port_open("127.0.0.1", vds_port):
        candidates.append(_normalize_endpoint({
            "name": "vds",
            "proxy": f"socks5://127.0.0.1:{vds_port}",
            "rate_limit": 3.0,
            "type": EndpointKind.PROXY.value,
            "kind": EndpointKind.PROXY.value,
            "driver": EndpointDriver.NATIVE.value,
            "slot_class": "socks5",
            "policy": _endpoint_policy(cfg, 3.0),
        }))

    return candidates


def _configured_runtime_candidates(cfg):
    runtime_types = _runtime_endpoint_types(cfg)
    raw = cfg.get("endpoints") or [{"name": "direct", "proxy": None, "rate_limit": 4.0, "type": "proxy"}]
    candidates = []

    for ep in raw:
        norm = _normalize_endpoint(ep)
        if norm["kind"] not in runtime_types:
            continue
        norm["driver"] = EndpointDriver.NATIVE.value
        norm["policy"] = _endpoint_policy(cfg, norm["rate_limit"], ep.get("policy"))
        candidates.append(norm)

    if not candidates and EndpointKind.PROXY.value in runtime_types:
        candidates.append(_normalize_endpoint({
            "name": "direct",
            "proxy": None,
            "rate_limit": 4.0,
            "type": EndpointKind.DIRECT.value,
            "kind": EndpointKind.DIRECT.value,
            "driver": EndpointDriver.NATIVE.value,
            "slot_class": "direct",
            "policy": _endpoint_policy(cfg, 4.0),
        }))
    return candidates


def _vpn_gateway_candidates(cfg):
    runtime_types = _runtime_endpoint_types(cfg)
    enabled = _experimental_drivers(cfg)
    runtime_enabled = (
        EndpointKind.BROWSER_GATEWAY.value in runtime_types and
        EndpointDriver.VPN_EXTENSION.value in enabled
    )
    candidates = []
    for vs in cfg.get("vpn_extensions", []):
        servers = vs.get("servers", [])
        if servers == ["auto"]:
            log.warning(f"[DISCOVER] {vs['extension']}: servers=auto not implemented in runtime, skip")
            continue
        for server in servers:
            rate_limit = vs.get("rate_limit", 2.0)
            candidates.append(_normalize_endpoint({
                "name": f"vpn-{vs['extension']}-{server}",
                "type": EndpointKind.BROWSER_GATEWAY.value,
                "kind": EndpointKind.BROWSER_GATEWAY.value,
                "driver": EndpointDriver.VPN_EXTENSION.value,
                "slot_class": "browser_gateway",
                "experimental": True,
                "runtime_enabled": runtime_enabled,
                "policy": _endpoint_policy(cfg, rate_limit, vs.get("policy")),
                "vpn_cfg": {
                    "extension": vs["extension"],
                    "server": server,
                    "path": vs.get("path"),
                    "headless": cfg.get("vpn_headless", False),
                },
            }))
    return candidates


def _web_proxy_candidates(cfg):
    runtime_types = _runtime_endpoint_types(cfg)
    enabled = _experimental_drivers(cfg)
    runtime_enabled = (
        cfg.get("web_proxy_enabled", False) and
        EndpointKind.BROWSER_GATEWAY.value in runtime_types and
        EndpointDriver.WEB_PROXY.value in enabled
    )
    candidates = []
    for candidate in cfg.get("web_proxy_candidates", []):
        landing = candidate["landing_url"]
        host = urlparse(landing).netloc or candidate["name"]
        candidates.append(_normalize_endpoint({
            "name": candidate["name"],
            "type": EndpointKind.BROWSER_GATEWAY.value,
            "kind": EndpointKind.BROWSER_GATEWAY.value,
            "driver": EndpointDriver.WEB_PROXY.value,
            "slot_class": "browser_gateway",
            "experimental": True,
            "runtime_enabled": runtime_enabled,
            "network_id": f"web-proxy:{host}",
            "policy": _endpoint_policy(cfg, candidate.get("rate_limit", 1.0), candidate.get("policy")),
            "web_proxy_cfg": dict(candidate),
        }))
    return candidates


def discover_configured_endpoints(cfg):
    if cfg.get("auto_discover", True):
        native = _discover_local_proxy_candidates(cfg)
    else:
        native = _configured_runtime_candidates(cfg)
    gateways = _vpn_gateway_candidates(cfg) + _web_proxy_candidates(cfg)
    candidates = native + gateways
    cfg["configured_endpoints"] = candidates
    return candidates


def discover_runtime_candidates(cfg):
    configured = discover_configured_endpoints(cfg)
    candidates = [ep for ep in configured if ep.get("runtime_enabled", True)]

    log.info(f"[DISCOVER] checking {len(candidates)} runtime candidates...")
    for candidate in candidates:
        if candidate["driver"] == EndpointDriver.NATIVE.value:
            target = candidate.get("proxy") or "base-ip"
        elif candidate["driver"] == EndpointDriver.VPN_EXTENSION.value:
            vpn = candidate["vpn_cfg"]
            target = f"{vpn['extension']}/{vpn['server']}"
        else:
            target = candidate["web_proxy_cfg"]["landing_url"]
        log.info(f"  {candidate['name']}: {target}")

    experimental = [
        ep["name"] for ep in configured
        if ep.get("experimental") and not ep.get("runtime_enabled", True)
    ]
    if experimental:
        log.info(
            f"[DISCOVER] experimental-only endpoints excluded from runtime: "
            f"{', '.join(experimental)}"
        )

    return candidates


async def preflight_endpoints(cfg, candidates=None, runtime_only=True):
    from patchright.async_api import async_playwright
    from scraper.vpn_ext import ensure_extensions

    candidates = candidates or (
        discover_runtime_candidates(cfg) if runtime_only else discover_configured_endpoints(cfg)
    )
    if not candidates:
        if runtime_only:
            cfg["verified_endpoints"] = []
        return []

    ensure_extensions(cfg)
    log.info(f"[PREFLIGHT] verifying {len(candidates)} endpoint candidates...")
    verified = []
    seen_network_ids = set()

    async with async_playwright() as pw:
        for candidate in candidates:
            retries = candidate.get("policy", {}).get("preflight_retries", cfg.get("endpoint_preflight_retries", 2))
            result = await _preflight_candidate(pw, candidate, retries=retries)
            if not result["ok"]:
                log.warning(f"  {candidate['name']}: rejected -- {result['reason']}")
                continue

            network_id = result["network_id"]
            if network_id in seen_network_ids:
                log.info(f"  {candidate['name']}: {network_id} (duplicate, skip)")
                continue

            seen_network_ids.add(network_id)
            ep = dict(candidate)
            ep["network_id"] = network_id
            ep["ip"] = network_id if network_id and "." in network_id else None
            ep["last_verify_status"] = "verified"
            ep["last_verify_reason"] = result["reason"]
            verified.append(ep)
            log.info(
                f"  {candidate['name']}: verified -- id={network_id} "
                f"lat={result['latency']:.1f}s"
            )

    if runtime_only:
        cfg["verified_endpoints"] = verified
        if verified:
            names = [f"{ep['name']}[{ep['slot_class']}:{ep.get('network_id', '-') }]" for ep in verified]
            log.info(f"[PREFLIGHT] runtime pool: {', '.join(names)}")
        else:
            log.error("[PREFLIGHT] no verified runtime endpoints")

    return verified


async def preflight_runtime_endpoints(cfg, candidates=None):
    return await preflight_endpoints(cfg, candidates=candidates, runtime_only=True)


async def resolve_runtime_endpoints(cfg):
    candidates = discover_runtime_candidates(cfg)
    if cfg.get("endpoint_preflight", True):
        verified = await preflight_runtime_endpoints(cfg, candidates)
    else:
        verified = [_normalize_endpoint(candidate) for candidate in candidates]
        cfg["verified_endpoints"] = verified

    if not verified:
        raise RuntimeError("no verified runtime endpoints after headless preflight")
    return verified


async def auto_discover(cfg):
    return await resolve_runtime_endpoints(cfg)


_vds_proc = None


def ensure_vds_tunnel(cfg):
    global _vds_proc

    host = os.getenv("VDS_HOST", "")
    user = os.getenv("VDS_USER", "")
    if not host or not user:
        return

    port = int(cfg.get("vds_socks_port", 9080))
    if _port_open("127.0.0.1", port):
        log.info(f"[VDS] tunnel already on :{port}")
        return

    if not _port_open(host, 22, timeout=5):
        log.info(f"[VDS] {host}:22 unreachable, skip")
        return

    key_paths = [
        Path.home() / ".ssh" / "id_ed25519",
    ]
    key = next((p for p in key_paths if p.exists()), None)
    if not key:
        log.info("[VDS] no SSH key, skip (ssh-keygen -t ed25519)")
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
