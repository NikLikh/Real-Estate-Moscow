import asyncio
import json
import logging
import signal
from contextlib import asynccontextmanager
from pathlib import Path

from config.settings import PROJECT_ROOT

log = logging.getLogger("re")

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
    path = PROJECT_ROOT / f".checkpoint_{name}.json"
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    tmp.replace(path)


def load_checkpoint(name: str) -> dict | None:
    path = PROJECT_ROOT / f".checkpoint_{name}.json"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def clear_checkpoint(name: str):
    path = PROJECT_ROOT / f".checkpoint_{name}.json"
    path.unlink(missing_ok=True)


def save_dead_letter(name: str, url: str, reason: str = ""):
    # url-ы которые упали больше N раз, потом можно разобраться вручную
    path = PROJECT_ROOT / f".dead_letters_{name}.json"
    letters = load_dead_letters(name)
    letters.append({"url": url, "reason": reason})
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(letters, f, ensure_ascii=False)
    tmp.replace(path)


def load_dead_letters(name: str) -> list[dict]:
    path = PROJECT_ROOT / f".dead_letters_{name}.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def clear_dead_letters(name: str):
    path = PROJECT_ROOT / f".dead_letters_{name}.json"
    path.unlink(missing_ok=True)
