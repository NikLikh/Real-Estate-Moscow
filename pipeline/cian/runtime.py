import asyncio
import json
import logging
import os
import signal
from datetime import date
from pathlib import Path

from config.settings import PROJECT_ROOT

log = logging.getLogger("re")

_CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
_CHECKPOINT_DIR.mkdir(exist_ok=True)

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


_PERSISTENT = {"cian_zhk"}


def _day() -> str:
    run_id = os.getenv("SCRAPE_RUN_ID") or ""
    if len(run_id) >= 8 and run_id[:8].isdigit():
        return f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}"
    return date.today().isoformat()


def save_checkpoint(name: str, state: dict):
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    tmp = path.with_suffix(".tmp")
    payload = dict(state)
    payload["_day"] = _day()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp.replace(path)


def load_checkpoint(name: str) -> dict | None:
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        state = json.load(f)
    day = state.pop("_day", None)
    if name not in _PERSISTENT and day != _day():
        log.info(f"checkpoint {name}: от другого дня, сбрасываю")
        path.unlink(missing_ok=True)
        return None
    return state


def clear_checkpoint(name: str):
    path = _CHECKPOINT_DIR / f".checkpoint_{name}.json"
    path.unlink(missing_ok=True)


def queue_snapshot(queue):
    try:
        return [item for item in list(queue._queue) if item is not None]
    except Exception:
        return []
