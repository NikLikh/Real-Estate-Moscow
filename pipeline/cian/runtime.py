import asyncio
import json
import logging
import signal
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
import time

from pipeline.cian.browser import (
    OFFER_EXTRA_BLOCKED,
    SessionIdentity,
    WebProxyPageAdapter,
    apply_cdp_blocking,
    create_stealth_context,
    handle_captcha,
    jittered_delay,
    reset_cdp_blocking,
    warmup_session,
)
from config.settings import PROJECT_ROOT

log = logging.getLogger("re")

_CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
_CHECKPOINT_DIR.mkdir(exist_ok=True)

# shutdown (ctrl+c) и restart (память/ошибки), воркеры проверяют should_stop()
_shutdown = asyncio.Event()
_restart = asyncio.Event()


def is_shutting_down() -> bool:
    return _shutdown.is_set()


def should_stop() -> bool:
    return _shutdown.is_set() or _restart.is_set()


def request_restart(reason: str):
    if not _restart.is_set() and not _shutdown.is_set():
        log.info(f"RESTART: {reason}")
        _restart.set()


def is_restarting() -> bool:
    return _restart.is_set() and not _shutdown.is_set()


def reset_restart():
    _restart.clear()


def install_shutdown_handler():
    def _handler(sig, frame):
        if not _shutdown.is_set():
            log.info("shutting down... finishing current tasks")
            _shutdown.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


@asynccontextmanager
async def managed_page(context):
    page = await context.new_page()
    try:
        yield page
    finally:
        try:
            await asyncio.wait_for(page.close(), timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass


def save_checkpoint(name: str, state: dict):
    # пишем через tmp чтобы не потерять файл при обрыве
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    tmp.replace(path)


def load_checkpoint(name: str) -> dict | None:
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def clear_checkpoint(name: str):
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    path.unlink(missing_ok=True)


def save_dead_letter(name: str, url: str, reason: str = ""):
    # url-ы которые упали больше N раз, потом можно разобраться вручную
    path = _CHECKPOINT_DIR / f".dead_letters_{name}.json"
    letters = load_dead_letters(name)
    letters.append({"url": url, "reason": reason})
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(letters, f, ensure_ascii=False)
    tmp.replace(path)


def load_dead_letters(name: str) -> list[dict]:
    path = _CHECKPOINT_DIR / f".dead_letters_{name}.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def clear_dead_letters(name: str):
    path = _CHECKPOINT_DIR / f".dead_letters_{name}.json"
    path.unlink(missing_ok=True)


class EndpointKind(StrEnum):
    DIRECT = "direct"
    PROXY = "proxy"
    BROWSER_GATEWAY = "browser_gateway"


class EndpointDriver(StrEnum):
    NATIVE = "native"
    VPN_EXTENSION = "vpn_extension"
    WEB_PROXY = "web_proxy"


class EndpointLifecycle(StrEnum):
    NEW = "new"
    WARMING = "warming"
    HEALTHY = "healthy"
    COOLDOWN = "cooldown"
    QUARANTINE = "quarantine"
    DEAD = "dead"


class EndpointEvent(StrEnum):
    SUCCESS = "success"
    WAF = "waf"
    WAF_RESOLVED = "waf_resolved"
    NETWORK = "network"
    CAPTCHA = "captcha"
    BUDGET = "budget"
    WARMUP_FAIL = "warmup_fail"
    DEAD = "dead"


class EndpointThrottle:

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


@dataclass(slots=True)
class EndpointPolicy:
    rate_limit: float = 2.0
    budget_limit: int = 9000
    budget_cooldown: float = 300.0
    waf_cooldown: float = 30.0
    network_cooldown: float = 20.0
    captcha_cooldown: float = 30.0
    quarantine_cooldown: float = 900.0
    max_warming_failures: int = 2
    max_quarantine_failures: int = 3
    preflight_retries: int = 2

    @classmethod
    def from_dict(cls, payload):
        if isinstance(payload, cls):
            return payload
        return cls(**(payload or {}))

    def to_dict(self):
        return {
            "rate_limit": self.rate_limit,
            "budget_limit": self.budget_limit,
            "budget_cooldown": self.budget_cooldown,
            "waf_cooldown": self.waf_cooldown,
            "network_cooldown": self.network_cooldown,
            "captcha_cooldown": self.captcha_cooldown,
            "quarantine_cooldown": self.quarantine_cooldown,
            "max_warming_failures": self.max_warming_failures,
            "max_quarantine_failures": self.max_quarantine_failures,
            "preflight_retries": self.preflight_retries,
        }


@dataclass(slots=True)
class EndpointSpec:
    name: str
    kind: str
    driver: str
    slot_class: str
    proxy: str | None = None
    runtime_enabled: bool = True
    experimental: bool = False
    network_id: str | None = None
    proxy_label: str | None = None
    vpn_cfg: dict | None = None
    web_proxy_cfg: dict | None = None
    policy: EndpointPolicy = field(default_factory=EndpointPolicy)
    last_verify_status: str = "unknown"
    last_verify_reason: str = ""

    @classmethod
    def from_dict(cls, payload):
        data = dict(payload)
        allowed = set(cls.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in allowed}
        data["policy"] = EndpointPolicy.from_dict(data.get("policy"))
        return cls(**data)

    def to_dict(self):
        return {
            "name": self.name,
            "kind": self.kind,
            "driver": self.driver,
            "slot_class": self.slot_class,
            "proxy": self.proxy,
            "runtime_enabled": self.runtime_enabled,
            "experimental": self.experimental,
            "network_id": self.network_id,
            "proxy_label": self.proxy_label,
            "vpn_cfg": self.vpn_cfg,
            "web_proxy_cfg": self.web_proxy_cfg,
            "policy": self.policy.to_dict(),
            "last_verify_status": self.last_verify_status,
            "last_verify_reason": self.last_verify_reason,
        }


@dataclass(slots=True)
class EndpointSnapshot:
    name: str
    kind: str
    driver: str
    slot_class: str
    lifecycle: str
    network_id: str | None
    proxy: str | None
    lease_owner: str | None
    lease_role: str | None
    requests: int
    budget_used: int
    successes: int
    waf_events: int
    captcha_events: int
    network_events: int
    cooldown_count: int
    quarantine_count: int
    batch_restarts: int
    warming_failures: int
    last_preflight_status: str
    last_preflight_reason: str
    last_transition_ts: float
    cooldown_until: float
    quarantine_until: float
    identity: dict

    def to_dict(self):
        return {
            "name": self.name,
            "kind": self.kind,
            "driver": self.driver,
            "slot_class": self.slot_class,
            "lifecycle": self.lifecycle,
            "network_id": self.network_id,
            "proxy": self.proxy,
            "lease_owner": self.lease_owner,
            "lease_role": self.lease_role,
            "requests": self.requests,
            "budget_used": self.budget_used,
            "successes": self.successes,
            "waf_events": self.waf_events,
            "captcha_events": self.captcha_events,
            "network_events": self.network_events,
            "cooldown_count": self.cooldown_count,
            "quarantine_count": self.quarantine_count,
            "batch_restarts": self.batch_restarts,
            "warming_failures": self.warming_failures,
            "last_preflight_status": self.last_preflight_status,
            "last_preflight_reason": self.last_preflight_reason,
            "last_transition_ts": self.last_transition_ts,
            "cooldown_until": self.cooldown_until,
            "quarantine_until": self.quarantine_until,
            "identity": self.identity,
        }


@dataclass(slots=True)
class RuntimeSessionPlan:
    browser_cap: int
    planner_workers: int
    max_concurrent: int
    listing_slots: list[str]
    offer_slots: list[str]
    retry_slots: list[str]
    total_offer: int
    total_retry: int
    n_browsers: int
    serial_offer_phase: bool = False

    def to_dict(self):
        return {
            "browser_cap": self.browser_cap,
            "planner_workers": self.planner_workers,
            "max_concurrent": self.max_concurrent,
            "listing_slots": list(self.listing_slots),
            "offer_slots": list(self.offer_slots),
            "retry_slots": list(self.retry_slots),
            "total_offer": self.total_offer,
            "total_retry": self.total_retry,
            "n_browsers": self.n_browsers,
            "serial_offer_phase": self.serial_offer_phase,
        }


class _EndpointState:

    def __init__(self, spec: EndpointSpec):
        self.spec = spec
        self.identity = SessionIdentity()
        self.throttle = EndpointThrottle(spec.policy.rate_limit)
        self.lifecycle = EndpointLifecycle.NEW
        self.network_id = spec.network_id
        self.lease_owner = None
        self.lease_role = None
        self.requests = 0
        self.budget_used = 0
        self.successes = 0
        self.waf_events = 0
        self.captcha_events = 0
        self.network_events = 0
        self.cooldown_count = 0
        self.quarantine_count = 0
        self.batch_restarts = 0
        self.warming_failures = 0
        self.last_preflight_status = spec.last_verify_status or "unknown"
        self.last_preflight_reason = spec.last_verify_reason or ""
        self.last_transition_ts = time.time()
        self.cooldown_until = 0.0
        self.quarantine_until = 0.0
        if spec.last_verify_status == "verified":
            self.lifecycle = EndpointLifecycle.HEALTHY

    @property
    def healthy(self):
        return self.lifecycle == EndpointLifecycle.HEALTHY

    @property
    def leased(self):
        return bool(self.lease_owner)

    @property
    def available(self):
        return self.healthy and not self.leased

    def endpoint_dict(self):
        return {
            "name": self.spec.name,
            "kind": self.spec.kind,
            "driver": self.spec.driver,
            "proxy": {"server": self.spec.proxy} if self.spec.proxy else None,
            "proxy_str": self.spec.proxy,
            "slot_class": self.spec.slot_class,
            "network_id": self.network_id,
            "ip": self.network_id,
            "policy": self.spec.policy.to_dict(),
            "vpn_cfg": self.spec.vpn_cfg,
            "web_proxy_cfg": self.spec.web_proxy_cfg,
            "experimental": self.spec.experimental,
            "runtime_enabled": self.spec.runtime_enabled,
            "last_verify_status": self.last_preflight_status,
            "last_verify_reason": self.last_preflight_reason,
            "lifecycle": self.lifecycle.value,
        }

    def snapshot(self):
        return EndpointSnapshot(
            name=self.spec.name,
            kind=self.spec.kind,
            driver=self.spec.driver,
            slot_class=self.spec.slot_class,
            lifecycle=self.lifecycle.value,
            network_id=self.network_id,
            proxy=self.spec.proxy,
            lease_owner=self.lease_owner,
            lease_role=self.lease_role,
            requests=self.requests,
            budget_used=self.budget_used,
            successes=self.successes,
            waf_events=self.waf_events,
            captcha_events=self.captcha_events,
            network_events=self.network_events,
            cooldown_count=self.cooldown_count,
            quarantine_count=self.quarantine_count,
            batch_restarts=self.batch_restarts,
            warming_failures=self.warming_failures,
            last_preflight_status=self.last_preflight_status,
            last_preflight_reason=self.last_preflight_reason,
            last_transition_ts=self.last_transition_ts,
            cooldown_until=self.cooldown_until,
            quarantine_until=self.quarantine_until,
            identity=self.identity.to_dict(),
        )

    def restore(self, payload):
        self.lifecycle = EndpointLifecycle(
            payload.get("lifecycle", self.lifecycle.value)
        )
        self.network_id = payload.get("network_id", self.network_id)
        self.lease_owner = payload.get("lease_owner")
        self.lease_role = payload.get("lease_role")
        self.requests = payload.get("requests", 0)
        self.budget_used = payload.get("budget_used", 0)
        self.successes = payload.get("successes", 0)
        self.waf_events = payload.get("waf_events", 0)
        self.captcha_events = payload.get("captcha_events", 0)
        self.network_events = payload.get("network_events", 0)
        self.cooldown_count = payload.get("cooldown_count", 0)
        self.quarantine_count = payload.get("quarantine_count", 0)
        self.batch_restarts = payload.get("batch_restarts", 0)
        self.warming_failures = payload.get("warming_failures", 0)
        self.last_preflight_status = payload.get(
            "last_preflight_status", self.last_preflight_status
        )
        self.last_preflight_reason = payload.get(
            "last_preflight_reason", self.last_preflight_reason
        )
        self.last_transition_ts = payload.get(
            "last_transition_ts", self.last_transition_ts
        )
        self.cooldown_until = payload.get("cooldown_until", 0.0)
        self.quarantine_until = payload.get("quarantine_until", 0.0)
        self.identity = SessionIdentity.from_dict(payload.get("identity"))


@dataclass(slots=True)
class EndpointLease:
    state: _EndpointState
    owner: str
    role: str
    acquired_at: float = field(default_factory=time.monotonic)

    @property
    def name(self):
        return self.state.spec.name

    @property
    def endpoint(self):
        return self.state.endpoint_dict()

    @property
    def throttle(self):
        return self.state.throttle

    @property
    def identity(self):
        return self.state.identity


class EndpointRegistry:

    def __init__(self, endpoints):
        self._states = {}
        for raw in endpoints:
            spec = raw if isinstance(raw, EndpointSpec) else EndpointSpec.from_dict(raw)
            self._states[spec.name] = _EndpointState(spec)

    def names(self):
        return list(self._states.keys())

    def get(self, name):
        return self._states[name]

    def restore(self, snapshots):
        for payload in snapshots or []:
            state = self._states.get(payload.get("name"))
            if not state:
                continue
            state.restore(payload)

    def runtime_endpoints(self):
        return [state.endpoint_dict() for state in self._states.values()]

    def snapshots(self):
        return [state.snapshot().to_dict() for state in self._states.values()]

    def healthy_endpoints(self):
        return [
            state.endpoint_dict() for state in self._states.values() if state.healthy
        ]

    def refresh(self):
        now = time.time()
        for state in self._states.values():
            if state.leased:
                continue
            if (
                state.lifecycle == EndpointLifecycle.COOLDOWN
                and now >= state.cooldown_until
            ):
                self._transition(state, EndpointLifecycle.WARMING, "cooldown elapsed")
            if (
                state.lifecycle == EndpointLifecycle.QUARANTINE
                and now >= state.quarantine_until
            ):
                self._transition(state, EndpointLifecycle.WARMING, "quarantine elapsed")

    def _transition(self, state, lifecycle, reason):
        state.lifecycle = lifecycle
        state.last_transition_ts = time.time()
        state.last_preflight_reason = reason

    def mark_healthy(self, name, reason, network_id=None):
        state = self.get(name)
        state.network_id = network_id or state.network_id
        state.last_preflight_status = "verified"
        state.last_preflight_reason = reason
        state.warming_failures = 0
        state.cooldown_until = 0.0
        state.quarantine_until = 0.0
        self._transition(state, EndpointLifecycle.HEALTHY, reason)

    def mark_warming(self, name, reason="warming"):
        state = self.get(name)
        self._transition(state, EndpointLifecycle.WARMING, reason)

    def mark_cooldown(self, name, reason, seconds):
        state = self.get(name)
        state.cooldown_count += 1
        state.cooldown_until = max(state.cooldown_until, time.time() + seconds)
        self._transition(state, EndpointLifecycle.COOLDOWN, reason)

    def mark_quarantine(self, name, reason):
        state = self.get(name)
        state.quarantine_count += 1
        if state.quarantine_count >= state.spec.policy.max_quarantine_failures:
            self.mark_dead(name, f"quarantine limit: {reason}")
            return
        state.quarantine_until = time.time() + state.spec.policy.quarantine_cooldown
        self._transition(state, EndpointLifecycle.QUARANTINE, reason)

    def mark_dead(self, name, reason):
        state = self.get(name)
        state.cooldown_until = 0.0
        state.quarantine_until = 0.0
        self._transition(state, EndpointLifecycle.DEAD, reason)

    def record_success(self, name):
        state = self.get(name)
        state.successes += 1
        if state.lifecycle in (EndpointLifecycle.WARMING, EndpointLifecycle.COOLDOWN):
            self.mark_healthy(name, "success")

    def record_request(self, name):
        state = self.get(name)
        state.requests += 1
        state.budget_used += 1
        if (
            state.spec.policy.budget_limit > 0
            and state.budget_used >= state.spec.policy.budget_limit
        ):
            state.budget_used = 0
            self.mark_cooldown(
                name, "budget exhausted", state.spec.policy.budget_cooldown
            )
            return True
        return False

    def record_batch_restart(self, name):
        state = self.get(name)
        state.batch_restarts += 1

    def record_waf(self, name, resolved=False):
        state = self.get(name)
        state.waf_events += 1
        if resolved:
            return
        self.mark_cooldown(name, "waf", state.spec.policy.waf_cooldown)

    def record_network(self, name):
        state = self.get(name)
        state.network_events += 1
        self.mark_cooldown(name, "network", state.spec.policy.network_cooldown)

    def record_captcha(self, name):
        state = self.get(name)
        state.captcha_events += 1
        self.mark_cooldown(name, "captcha", state.spec.policy.captcha_cooldown)

    def record_warmup_failure(self, name, reason):
        state = self.get(name)
        state.last_preflight_status = "failed"
        state.last_preflight_reason = reason
        state.warming_failures += 1
        if state.warming_failures >= state.spec.policy.max_warming_failures:
            self.mark_quarantine(name, reason)
            return
        self.mark_cooldown(name, reason, state.spec.policy.network_cooldown)

    def acquire(self, name, owner, role, shared=False):
        state = self.get(name)
        if not shared and not state.available:
            return None
        if shared and not state.healthy:
            return None
        state.lease_owner = owner
        state.lease_role = role
        return EndpointLease(state=state, owner=owner, role=role)

    def release(self, lease: EndpointLease):
        state = lease.state
        if state.lease_owner == lease.owner:
            state.lease_owner = None
            state.lease_role = None

    def format_lines(self):
        lines = []
        now = time.time()
        for state in self._states.values():
            extra = []
            if (
                state.lifecycle == EndpointLifecycle.COOLDOWN
                and state.cooldown_until > now
            ):
                extra.append(f"cool={int(state.cooldown_until - now)}s")
            if (
                state.lifecycle == EndpointLifecycle.QUARANTINE
                and state.quarantine_until > now
            ):
                extra.append(f"quar={int(state.quarantine_until - now)}s")
            if state.lease_owner:
                extra.append(f"lease={state.lease_owner}:{state.lease_role}")
            extra.append(f"req={state.requests}")
            extra.append(f"waf={state.waf_events}")
            extra.append(f"net={state.network_events}")
            lines.append(
                f"{state.spec.name}[{state.spec.kind}/{state.spec.driver}] "
                f"{state.lifecycle.value} ip={state.network_id or '-'} {' '.join(extra)}"
            )
        return lines


class EndpointOrchestrator:

    def __init__(self, registry: EndpointRegistry, cfg=None):
        self.registry = registry
        self.cfg = cfg or {}
        self._lock = asyncio.Lock()
        self._notify = asyncio.Condition()
        self._indexes = {}
        self._waf_times = []
        self._cb_window = self.cfg.get("cb_window", 30)
        self._cb_threshold = self.cfg.get("cb_threshold", 6)
        self._cb_cooldown = self.cfg.get("cb_cooldown", 90)
        self._cb_until = 0.0

    def _pick(self, candidates, key):
        if not candidates:
            return None
        idx = self._indexes.get(key, 0)
        state = candidates[idx % len(candidates)]
        self._indexes[key] = idx + 1
        return state

    async def acquire(
        self, owner, role, allowed_names=None, prefer_name=None, shared=False
    ):
        allowed = set(allowed_names or [])
        while True:
            async with self._lock:
                self.registry.refresh()
                picked = None
                if prefer_name:
                    state = self.registry.get(prefer_name)
                    # shared=True: несколько workers на одном endpoint (listing при http_offers)
                    if (
                        state.healthy
                        and (shared or not state.leased)
                        and (not allowed or state.spec.name in allowed)
                    ):
                        picked = state
                if not picked:
                    candidates = [
                        state
                        for state in self.registry._states.values()
                        if state.healthy
                        and (shared or not state.leased)
                        and (not allowed or state.spec.name in allowed)
                    ]
                    key = tuple(sorted(allowed)) if allowed else "all"
                    picked = self._pick(candidates, key)
                if picked:
                    return self.registry.acquire(
                        picked.spec.name, owner, role, shared=shared
                    )
            await asyncio.sleep(1)

    async def release(self, lease):
        async with self._lock:
            self.registry.release(lease)

    async def rotate(
        self, lease, event: str, reason: str, allowed_names=None, prefer_name=None
    ):
        async with self._lock:
            self._apply_event_locked(lease, event, reason)
            self.registry.release(lease)
        return await self.acquire(
            lease.owner,
            lease.role,
            allowed_names=allowed_names,
            prefer_name=prefer_name,
        )

    def _apply_event_locked(self, lease, event: str, reason: str):
        name = lease.name
        if event == EndpointEvent.SUCCESS:
            self.registry.record_success(name)
            return
        if event == EndpointEvent.WAF_RESOLVED:
            self.registry.record_waf(name, resolved=True)
            return
        if event == EndpointEvent.WAF:
            self.registry.record_waf(name, resolved=False)
            now = time.monotonic()
            self._waf_times = [t for t in self._waf_times if now - t < self._cb_window]
            self._waf_times.append(now)
            if len(self._waf_times) >= self._cb_threshold:
                self._cb_until = time.monotonic() + self._cb_cooldown
                self._waf_times.clear()
                log.warning(
                    f"[CIRCUIT] OPEN, все воркеры на паузе {self._cb_cooldown}s"
                )
            return
        if event == EndpointEvent.NETWORK:
            self.registry.record_network(name)
            return
        if event == EndpointEvent.CAPTCHA:
            self.registry.record_captcha(name)
            return
        if event == EndpointEvent.BUDGET:
            self.registry.mark_cooldown(
                name, reason or "budget", lease.state.spec.policy.budget_cooldown
            )
            return
        if event == EndpointEvent.WARMUP_FAIL:
            self.registry.record_warmup_failure(name, reason)
            return
        if event == EndpointEvent.DEAD:
            self.registry.mark_dead(name, reason or "dead")

    async def report_event(self, lease, event: str, reason=""):
        async with self._lock:
            self._apply_event_locked(lease, event, reason)

    async def report_request(self, lease):
        async with self._lock:
            return self.registry.record_request(lease.name)

    async def report_success(self, lease):
        async with self._lock:
            self.registry.record_success(lease.name)

    async def report_batch_restart(self, lease):
        async with self._lock:
            self.registry.record_batch_restart(lease.name)

    async def wait_if_open(self):
        if time.monotonic() >= self._cb_until:
            return False
        remaining = self._cb_until - time.monotonic()
        if remaining > 0:
            log.info(f"[CIRCUIT] waiting {remaining:.0f}s...")
            await asyncio.sleep(remaining)
        log.info("[CIRCUIT] CLOSED, продолжаем")
        return True


def queue_snapshot(queue):
    try:
        return [item for item in list(queue._queue) if item is not None]
    except Exception:
        return []


def build_runtime_session_plan(
    cfg, remaining_filters, endpoint_snapshots, http_offers=False
):
    healthy = [
        ep
        for ep in endpoint_snapshots
        if ep.get("lifecycle") == EndpointLifecycle.HEALTHY.value
    ]
    endpoint_names = [ep["name"] for ep in healthy]
    healthy_count = len(endpoint_names)
    if healthy_count == 0:
        raise RuntimeError("no healthy endpoints for runtime session plan")

    if http_offers:
        # curl_cffi парсит offers без browser, browser нужен только для planner
        browser_cap = max(1, cfg.get("browser_pool_cap", 8))
        max_concurrent = cfg.get("max_concurrent", 8)
    else:
        browser_cap = max(
            1, min(cfg.get("browser_pool_cap", healthy_count), healthy_count)
        )
        max_concurrent = min(cfg.get("max_concurrent", browser_cap), browser_cap)
    planner_workers = max(1, min(cfg.get("planner_workers", 3), browser_cap))

    if http_offers:
        # curl_cffi парсит offers, listing workers на base IP (экономим VPN для offers)
        n_listing = min(cfg.get("listing_workers", 4), max(1, remaining_filters))
        base_ep = cfg.get("listing_endpoint", "direct")
        if base_ep not in endpoint_names:
            base_ep = endpoint_names[0]
        listing_slots = [base_ep] * n_listing
        offer_slots = []
        retry_slots = []
        serial_offer_phase = False
    elif healthy_count == 1:
        listing_slots = endpoint_names[:1] if remaining_filters else []
        offer_slots = []
        retry_slots = []
        serial_offer_phase = True
    else:
        max_listing = max(1, healthy_count - 1)
        listing_count = min(
            cfg.get("listing_workers", 2), max_listing, max(1, remaining_filters)
        )
        listing_slots = endpoint_names[:listing_count]
        free_names = endpoint_names[listing_count:]
        retry_count = min(cfg.get("retry_workers", 1), max(0, len(free_names) - 1))
        retry_slots = free_names[:retry_count]
        offer_slots = free_names[retry_count:]
        serial_offer_phase = not offer_slots

    active_sessions = max(1, len(listing_slots) + len(offer_slots) + len(retry_slots))
    n_browsers = max(1, min(browser_cap, max(planner_workers, active_sessions)))

    return RuntimeSessionPlan(
        browser_cap=browser_cap,
        planner_workers=planner_workers,
        max_concurrent=max_concurrent,
        listing_slots=listing_slots,
        offer_slots=offer_slots,
        retry_slots=retry_slots,
        total_offer=len(offer_slots),
        total_retry=len(retry_slots),
        n_browsers=n_browsers,
        serial_offer_phase=serial_offer_phase,
    )


NETWORK_ERRORS = (
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_HTTP2_PING_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_NETWORK_CHANGED",
    "ERR_SOCKET_NOT_CONNECTED",
)


async def throttled_goto(page, url, sem, timeout=120000, throttle=None):
    if throttle:
        await throttle.wait()
    async with sem:
        t0 = time.monotonic()
        try:
            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            return True, time.monotonic() - t0
        except Exception as e:
            msg = str(e)
            log.debug(f"    goto failed: {msg[:60]}")
            dt = time.monotonic() - t0
            if any(err in msg for err in NETWORK_ERRORS):
                return "network", dt
            return False, dt


async def _close_ctx(ctx, page):
    try:
        await page.close()
    except Exception:
        pass
    try:
        await ctx.close()
    except Exception:
        pass


class EndpointSession:

    _DEFAULT_REFERER = (
        "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat"
    )

    def __init__(
        self,
        name,
        role,
        browser,
        orchestrator,
        *,
        block_extra=True,
        allowed_names=None,
        prefer_name=None,
        pw=None,
        cfg=None,
        shared=False,
        do_warmup=True,
        referer=None,
    ):
        self.name = name
        self.role = role
        self.browser = browser
        self.orchestrator = orchestrator
        self.block_extra = block_extra
        self.allowed_names = list(allowed_names or [])
        self.prefer_name = prefer_name
        self.pw = pw
        self.cfg = cfg or {}
        self.do_warmup = do_warmup
        self.shared = shared
        self.referer = referer
        self.lease = None
        self.ep = None
        self.ctx = None
        self.raw_page = None
        self.page = None
        self.cdp = None
        self._req_count = 0
        self._budget_hit = False
        self._personal_throttle = EndpointThrottle(max_per_sec=0.4)

    async def _acquire(self, prefer_name=None, allowed_names=None):
        self.lease = await self.orchestrator.acquire(
            self.name,
            self.role,
            allowed_names=allowed_names or self.allowed_names or None,
            prefer_name=prefer_name if prefer_name is not None else self.prefer_name,
            shared=self.shared,
        )
        self.ep = self.lease.endpoint

    async def open(self):
        while True:
            if not self.lease:
                await self._acquire()
            try:
                await self._open_current()
                await self.orchestrator.report_success(self.lease)
                self._req_count = 0
                self._budget_hit = False
                return
            except Exception as e:
                reason = str(e)[:120]
                await self._safe_close()
                await self.orchestrator.report_event(
                    self.lease, EndpointEvent.WARMUP_FAIL, reason
                )
                await self.orchestrator.release(self.lease)
                self.lease = None
                self.ep = None
                if should_stop():
                    raise

    async def _open_current(self):
        driver = self.ep["driver"]
        if driver == EndpointDriver.VPN_EXTENSION.value:
            await self._open_vpn()
            return

        proxy = self.ep.get("proxy")
        self.ctx = await create_stealth_context(
            self.browser,
            proxy=proxy,
            identity=self.lease.identity,
        )
        ref = self.referer if self.referer is not None else self._DEFAULT_REFERER
        if ref:
            await self.ctx.set_extra_http_headers({"Referer": ref})
        self.raw_page = await self.ctx.new_page()

        if driver == EndpointDriver.WEB_PROXY.value:
            await apply_cdp_blocking(self.raw_page)
            self.page = WebProxyPageAdapter(self.raw_page, self.ep["web_proxy_cfg"])
            if self.do_warmup:
                await self.page.goto(self.ep["web_proxy_cfg"]["target_url"])
            self.cdp = None
            return

        if self.do_warmup:
            cdp_warmup = await apply_cdp_blocking(self.raw_page)
            await warmup_session(self.raw_page)
            if cdp_warmup:
                await reset_cdp_blocking(cdp_warmup)

        extra = OFFER_EXTRA_BLOCKED if self.block_extra else ()
        self.cdp = await apply_cdp_blocking(self.raw_page, extra_patterns=extra)
        self.page = self.raw_page

    async def _open_vpn(self):
        from pipeline.cian.vpn_ext import launch_vpn_context

        vpn = self.ep["vpn_cfg"]
        self.ctx, _ = await launch_vpn_context(
            self.pw,
            vpn["extension"],
            vpn["server"],
            identity=self.lease.identity,
            headless=vpn.get("headless", False),
            cfg_path=vpn.get("path"),
        )
        ref = self.referer if self.referer is not None else self._DEFAULT_REFERER
        if ref:
            await self.ctx.set_extra_http_headers({"Referer": ref})
        self.raw_page = (
            self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
        )
        if self.do_warmup:
            cdp_warmup = await apply_cdp_blocking(self.raw_page)
            await warmup_session(self.raw_page)
            if cdp_warmup:
                await reset_cdp_blocking(cdp_warmup)
        extra = OFFER_EXTRA_BLOCKED if self.block_extra else ()
        self.cdp = await apply_cdp_blocking(self.raw_page, extra_patterns=extra)
        self.page = self.raw_page

    async def _safe_close(self):
        if not self.ctx:
            return
        if self.ep and self.ep["driver"] == EndpointDriver.VPN_EXTENSION.value:
            try:
                await self.ctx.close()
            except Exception:
                pass
        elif self.raw_page:
            await _close_ctx(self.ctx, self.raw_page)
        self.ctx = None
        self.raw_page = None
        self.page = None
        self.cdp = None

    async def close(self):
        await self._safe_close()
        if self.lease:
            await self.orchestrator.release(self.lease)
            self.lease = None
            self.ep = None

    async def reopen(self, prefer_name=None):
        current_name = self.lease.name if self.lease else None
        allowed = self.allowed_names or ([current_name] if current_name else None)
        await self._safe_close()
        if self.lease:
            await self.orchestrator.release(self.lease)
            self.lease = None
        await self._acquire(
            prefer_name=prefer_name or current_name, allowed_names=allowed
        )
        await self._open_current()
        await self.orchestrator.report_success(self.lease)
        self._req_count = 0
        self._budget_hit = False

    async def rotate(self, event, reason="", prefer_name=None):
        old_name = self.lease.name if self.lease else None
        await self._safe_close()
        if self.lease:
            self.lease = await self.orchestrator.rotate(
                self.lease,
                event,
                reason,
                allowed_names=self.allowed_names or None,
                prefer_name=prefer_name,
            )
            self.ep = self.lease.endpoint
            log.info(
                f"[{self.name}] {reason or event}, ротация {old_name} на {self.lease.name}"
            )
        await self._open_current()
        self._req_count = 0
        self._budget_hit = False

    async def restart_batch(self, cooldown):
        current_name = self.lease.name if self.lease else None
        if self.lease:
            await self.orchestrator.report_batch_restart(self.lease)
        await self._safe_close()
        await jittered_delay(*cooldown)
        if self.lease:
            await self.orchestrator.release(self.lease)
            self.lease = None
        await self._acquire(
            prefer_name=current_name,
            allowed_names=self.allowed_names
            or ([current_name] if current_name else None),
        )
        await self._open_current()
        await self.orchestrator.report_success(self.lease)
        self._req_count = 0
        self._budget_hit = False

    async def maybe_restart_batch(self, batch_size, cooldown):
        self._req_count += 1
        if self._req_count > batch_size:
            log.info(f"[{self.name}] batch done ({batch_size}), restarting context")
            await self.restart_batch(cooldown)

    async def goto(self, url, sem, timeout=30000):
        if self._budget_hit:
            await self.rotate(EndpointEvent.BUDGET, "budget exhausted")

        was_paused = await self.orchestrator.wait_if_open()
        if was_paused:
            log.info(f"[{self.name}] post-CB reset, fresh context")
            await self.reopen()

        await self._personal_throttle.wait()
        throttle = self.lease.throttle if self.lease else None
        result = await throttled_goto(
            self.page, url, sem, timeout=timeout, throttle=throttle
        )
        if self.lease:
            self._budget_hit = await self.orchestrator.report_request(self.lease)
        return result

    async def handle_captcha(self, url):
        if not self.ctx:
            return False

        if self.ep["driver"] == EndpointDriver.WEB_PROXY.value:
            cap_page = await self.ctx.new_page()
            try:
                adapter = WebProxyPageAdapter(cap_page, self.ep["web_proxy_cfg"])
                await adapter.goto(url)
                return await handle_captcha(adapter)
            finally:
                try:
                    await cap_page.close()
                except Exception:
                    pass

        cap_page = await self.ctx.new_page()
        await apply_cdp_blocking(cap_page)
        try:
            await cap_page.goto(url, timeout=30000, wait_until="domcontentloaded")
            return await handle_captcha(cap_page)
        except Exception:
            return False
        finally:
            try:
                await cap_page.close()
            except Exception:
                pass

    async def report_success(self):
        if self.lease:
            await self.orchestrator.report_success(self.lease)

    async def report_waf_resolved(self):
        if self.lease:
            await self.orchestrator.report_event(
                self.lease, EndpointEvent.WAF_RESOLVED, "waf resolved"
            )
