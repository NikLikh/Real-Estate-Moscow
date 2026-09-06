import asyncio
import json
import logging
import re
from itertools import zip_longest

from curl_cffi.requests import AsyncSession

from config.settings import PROJECT_ROOT

log = logging.getLogger("re")

_CACHE = PROJECT_ROOT / "checkpoints" / "proxies_alive.json"

_LISTS = [
    ("monosans", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json", "json", "socks5"),
    ("speedx5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "txt", "socks5"),
    ("hookzof", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "txt", "socks5"),
    ("proxifly", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt", "txt", "socks5"),
    ("zloi", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt", "txt", "socks5"),
    ("proxyscrape5", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000", "txt", "socks5"),
    ("geonode", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&protocols=socks5", "geonode", "socks5"),
    ("monosans5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "txt", "socks5"),
    ("prxchk5", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt", "txt", "socks5"),
    ("jetkai", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "txt", "socks5"),
    ("ercin", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt", "txt", "socks5"),
    ("zaeem", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt", "txt", "socks5"),
    ("sunny9577", "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt", "txt", "socks5"),
    ("vakhov", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt", "txt", "socks5"),
    ("roosterkid", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "txt", "socks5"),
    ("openproxylist", "https://openproxylist.xyz/socks5.txt", "txt", "socks5"),
    ("proxyspace", "https://proxyspace.pro/socks5.txt", "txt", "socks5"),
    ("mmpx12", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "txt", "socks5"),
    ("anonym0us", "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt", "txt", "socks5"),
    ("tuanminpay", "https://raw.githubusercontent.com/tuanminpay/live-proxy/master/socks5.txt", "txt", "socks5"),
    ("elliottophellia", "https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks5/global/socks5_checked.txt", "txt", "socks5"),
    ("themiralay", "https://raw.githubusercontent.com/themiralay/Proxy-List-World/master/data.txt", "txt", "socks5"),
    ("proxyscan", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt", "txt", "socks5"),
    ("hendrikbgr", "https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt", "txt", "socks5"),
    ("rdavydov", "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt", "txt", "socks5"),
    ("yemixzy", "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/socks5.txt", "txt", "socks5"),
    ("dpangestuw", "https://raw.githubusercontent.com/dpangestuw/Free-Proxy/refs/heads/main/socks5_proxies.txt", "txt", "socks5"),
    ("ppy", "https://raw.githubusercontent.com/casals-ar/proxy-list/main/socks5", "txt", "socks5"),
    ("gfpcom", "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/list/socks5/global.txt", "txt", "socks5"),
    ("speedx4", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "txt", "socks4"),
    ("rdavydov4", "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks4.txt", "txt", "socks4"),
    ("jetkai4", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "txt", "socks4"),
    ("ercin4", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt", "txt", "socks4"),
    ("monosans4", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "txt", "socks4"),
    ("prxchk4", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt", "txt", "socks4"),
    ("proxyscrape4", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000", "txt", "socks4"),
    ("zloi4", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt", "txt", "socks4"),
]

_RE_HOSTPORT = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})")


async def _grab(name, url, kind, scheme):
    try:
        async with AsyncSession(impersonate="chrome") as s:
            resp = await s.get(url, timeout=30)
            if kind == "json":
                pairs = [
                    (it["host"], it["port"])
                    for it in resp.json()
                    if it.get("protocol") == "socks5" and it.get("host") and it.get("port")
                ]
            elif kind == "geonode":
                pairs = [
                    (it["ip"], it["port"])
                    for it in resp.json().get("data", [])
                    if it.get("ip") and it.get("port")
                ]
            else:
                pairs = _RE_HOSTPORT.findall(resp.text)
        return [f"{scheme}://{h}:{p}" for h, p in pairs]
    except Exception as e:
        log.warning(f"[HTTP] список {name}: {type(e).__name__}")
        return []


async def fetch_candidates(cfg=None):
    cfg = cfg or {}
    cap = cfg.get("proxy_list_cap", 0) or None
    grabbed = await asyncio.gather(*(_grab(n, u, k, s) for n, u, k, s in _LISTS))
    live_lists = [lst[:cap] for lst in grabbed if lst]
    order = [p for row in zip_longest(*live_lists) for p in row if p]
    uniq = list(dict.fromkeys(order))
    log.info(f"[HTTP] списки: {sum(1 for l in grabbed if l)}/{len(_LISTS)} отдали, {len(uniq)} кандидатов")
    return uniq


def load_cache() -> list[str]:
    if not _CACHE.exists():
        return []
    try:
        with open(_CACHE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if isinstance(data, dict):
        data = list(data)
    return [(p if "://" in p else f"socks5://{p}") for p in data]


def save_cache(alive):
    try:
        with open(_CACHE, "w", encoding="utf-8") as f:
            json.dump(list(alive), f)
    except Exception as e:
        log.debug(f"[HTTP] proxy cache write: {e}")
