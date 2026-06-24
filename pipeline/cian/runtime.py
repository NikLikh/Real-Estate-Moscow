import asyncio
import json
import logging
import signal
from pathlib import Path

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


def queue_snapshot(queue):
    try:
        return [item for item in list(queue._queue) if item is not None]
    except Exception:
        return []
