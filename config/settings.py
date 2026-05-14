import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


load_dotenv(PROJECT_ROOT / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "real_estate"),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
}

JDBC_URL = (
    f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
}

_jars = PROJECT_ROOT / "jars" / "postgresql-42.7.4.jar"
JARS_PATH = (
    str(_jars).replace("Никита", "Nikita") if "Никита" in str(_jars) else str(_jars)
)

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("re")


# кэшируем чтобы не парсить yaml на каждый вызов
_scraper_cfg = None


def _env_bool(name):
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip().lower()
    return value in {"1", "true", "yes", "on"}


def _env_int(name):
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return int(value)


def _env_csv(name):
    value = os.getenv(name)
    if not value:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def _apply_scraper_env_overrides(cfg):
    overrides = {
        "auto_discover": _env_bool("SCRAPER_AUTO_DISCOVER"),
        "endpoint_preflight": _env_bool("SCRAPER_ENDPOINT_PREFLIGHT"),
        "headless": _env_bool("SCRAPER_HEADLESS"),
        "vpn_headless": _env_bool("SCRAPER_VPN_HEADLESS"),
        "web_proxy_enabled": _env_bool("SCRAPER_WEB_PROXY_ENABLED"),
        "vless_socks_port": _env_int("VLESS_SOCKS_PORT")
        or _env_int("SCRAPER_VLESS_SOCKS_PORT"),
        "runtime_endpoint_types": _env_csv("SCRAPER_RUNTIME_ENDPOINT_TYPES"),
        "experimental_endpoint_types": _env_csv("SCRAPER_EXPERIMENTAL_ENDPOINT_TYPES"),
    }
    for key, value in overrides.items():
        if value is not None:
            cfg[key] = value
    return cfg


def load_scraper_config(path=None):
    global _scraper_cfg
    if _scraper_cfg is not None and path is None:
        return _scraper_cfg

    p = Path(path) if path else PROJECT_ROOT / "config" / "scraper.yaml"
    with open(p, encoding="utf-8") as f:
        _scraper_cfg = yaml.safe_load(f)
    _scraper_cfg = _apply_scraper_env_overrides(_scraper_cfg)
    return _scraper_cfg
