import logging
import uuid as uuid_mod

from curl_cffi.requests import AsyncSession

from pipeline.cian.proxy_farm.validator import validate_batch_s1s2

log = logging.getLogger("re")

_API = "https://1clickvpn.net/api/v1/servers/"


async def discover(cfg) -> list[tuple[str, str, str]]:
    client_uuid = cfg.get("oneclickvpn_uuid") or str(uuid_mod.uuid4())
    if not cfg.get("oneclickvpn_uuid"):
        cfg["oneclickvpn_uuid"] = client_uuid

    try:
        async with AsyncSession(impersonate="chrome") as s:
            resp = await s.get(f"{_API}?c={client_uuid}", timeout=10)
            servers = resp.json()
    except Exception as e:
        log.warning(f"[HTTP] 1clickvpn: API error: {e}")
        return []

    if not isinstance(servers, list):
        log.warning("[HTTP] 1clickvpn: unexpected response format")
        return []

    candidates = []
    for srv in servers:
        creds = srv.get("credentials", {})
        user = creds.get("username")
        pwd = creds.get("password")
        if not user or not pwd:
            continue
        cc = srv.get("countryCode", "xx").lower()
        for node in srv.get("nodes", []):
            host = node.get("host")
            port = node.get("port", 443)
            if not host:
                continue
            proxy_url = f"https://{user}:{pwd}@{host}:{port}"
            label = f"1click-{cc}-{host}:{port}"
            candidates.append((label, proxy_url))

    if not candidates:
        return []

    log.info(f"[HTTP] 1clickvpn: {len(candidates)} candidates, validating S1+S2...")

    result = await validate_batch_s1s2(candidates, concurrency=5, timeout=10)
    log.info(f"[HTTP] 1clickvpn: {len(result)}/{len(candidates)} passed S1+S2")
    return result
