import functools
import logging
import os
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# defaults подставятся если .env нет, удобно для первого запуска
load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "real_estate"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

# PySpark пишет в ту же БД, но через JDBC
JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
}

JARS_PATH = str(PROJECT_ROOT / "jars" / "postgresql-42.7.4.jar")

# один раз на весь проект, формат без лишнего
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("re")


def timed(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        t0 = time.monotonic()
        result = fn(*args, **kwargs)
        dt = time.monotonic() - t0
        log.info(f"{fn.__name__} took {dt:.1f}s")
        return result

    return wrapper


# кэшируем чтобы не парсить yaml на каждый вызов
_scraper_cfg = None


def load_scraper_config(path=None):
    global _scraper_cfg
    if _scraper_cfg is not None and path is None:
        return _scraper_cfg

    p = Path(path) if path else PROJECT_ROOT / "config" / "scraper.yaml"
    with open(p, encoding="utf-8") as f:
        _scraper_cfg = yaml.safe_load(f)
    return _scraper_cfg
