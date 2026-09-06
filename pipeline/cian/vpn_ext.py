
import asyncio
import io
import logging
import random
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from playwright_stealth import Stealth

log = logging.getLogger("re")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1536, "height": 864},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1600, "height": 900},
]


class SessionIdentity:
    def __init__(self, user_agent=None, viewport=None, scale=None):
        self.user_agent = user_agent or random.choice(USER_AGENTS)
        self.viewport = dict(viewport or random.choice(VIEWPORTS))
        self.scale = scale if scale is not None else random.choice([1, 1.25, 1.5])


CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-component-extensions-with-background-pages",
    "--blink-settings=imagesEnabled=false",
    "--disable-http2",
    "--no-proxy-server",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--disable-software-rasterizer",
    "--disable-background-networking",
    "--disable-sync",
    "--disable-translate",
    "--disable-notifications",
    "--disable-default-apps",
    "--disable-popup-blocking",
    "--no-sandbox",
    "--disable-infobars",
    "--disable-session-crashed-bubble",
    "--disable-features=TranslateUI",
    "--metrics-recording-only",
    "--mute-audio",
]

CHROMIUM_ARGS_VPN = [
    a
    for a in CHROMIUM_ARGS
    if a
    not in ("--disable-component-extensions-with-background-pages", "--no-proxy-server")
]

EXT_BASE = Path(__file__).resolve().parent

_stealth = Stealth(
    navigator_languages_override=["ru-RU", "ru", "en-US", "en"],
)

_CWS_URL = (
    "https://clients2.google.com/service/update2/crx"
    "?response=redirect&prodversion=136.0&acceptformat=crx2,crx3"
    "&x=id%3D{ext_id}%26uc"
)

EXTENSIONS = {
    "browsec": {
        "webstore_id": "omghfjlpggmjjaagoclmmobgdodcjboh",
        "connect_js": """async (country) => {
            const items = await new Promise(r => chrome.storage.local.get('lowLevelPac', r));
            const pac = items['lowLevelPac'];
            if (!pac || !pac.countries) return 'no PAC data';

            const servers = pac.countries[country];
            if (!servers || !servers.length) return 'no servers for ' + country;

            const idx = Math.floor(Math.random() * servers.length);
            const raw = servers[idx];
            const server = raw.replace(/^HTTPS /, '').replace(/^HTTP /, '');
            await proxy.setSingleServer(server);
            return server;
        }""",
        "check_connected_js": """async () => {
            const pac = await proxy.getPac();
            return pac !== '' && pac !== null;
        }""",
    },
    "cyberghost": {
        "webstore_id": "ffbkglfijbcbgblgflchnbphjdllaogb",
    },
}

_temp_dirs = []


def _make_temp_dir(label):
    d = Path(tempfile.mkdtemp(prefix=f"vpn_{label}_"))
    _temp_dirs.append(d)
    return d


def cleanup_temp_dirs():
    for d in _temp_dirs:
        try:
            shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass
    _temp_dirs.clear()


def download_extension(ext_name):
    ext_cfg = EXTENSIONS.get(ext_name)
    if not ext_cfg or "webstore_id" not in ext_cfg:
        raise ValueError(f"no webstore_id for extension: {ext_name}")

    dest = EXT_BASE /"extensions" / ext_name
    if (dest / "manifest.json").exists():
        return dest

    url = _CWS_URL.format(ext_id=ext_cfg["webstore_id"])
    log.info(f"[VPN] downloading {ext_name} from Chrome Web Store...")

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=30)
    crx_data = resp.read()

    if not crx_data[:4] == b"Cr24":
        raise ValueError(f"downloaded file is not a valid CRX (got {crx_data[:20]})")

    pk_offset = crx_data.find(b"PK\x03\x04")
    if pk_offset < 0:
        raise ValueError("CRX does not contain ZIP data")

    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(crx_data[pk_offset:])) as zf:
        zf.extractall(dest)

    log.info(f"[VPN] {ext_name} unpacked to {dest}")
    return dest


def ensure_extensions(cfg):
    for vs in cfg.get("vpn_extensions", []):
        ext_name = vs["extension"]
        cfg_path = vs.get("path")

        if find_extension_path(ext_name, cfg_path=cfg_path):
            log.info(f"[VPN] {ext_name}: found")
            continue

        if ext_name in EXTENSIONS and "webstore_id" in EXTENSIONS[ext_name]:
            try:
                download_extension(ext_name)
            except Exception as e:
                log.error(f"[VPN] failed to download {ext_name}: {e}")
        else:
            log.warning(f"[VPN] {ext_name}: not found and no auto-download available")


def unpack_crx(crx_path, dest_dir):
    crx_path = Path(crx_path)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(crx_path) as zf:
            zf.extractall(dest_dir)
            return dest_dir
    except zipfile.BadZipFile:
        pass

    data = crx_path.read_bytes()
    pk_offset = data.find(b"PK\x03\x04")
    if pk_offset < 0:
        raise ValueError(f"not a valid CRX/ZIP: {crx_path}")

    with zipfile.ZipFile(io.BytesIO(data[pk_offset:])) as zf:
        zf.extractall(dest_dir)
    return dest_dir


def find_extension_path(ext_name, cfg_path=None):
    if cfg_path:
        p = Path(cfg_path)
        if not p.is_absolute():
            p = EXT_BASE /p
        if (p / "manifest.json").exists():
            return p

    default = EXT_BASE /"extensions" / ext_name
    if (default / "manifest.json").exists():
        return default

    return None


async def _find_ext_worker(ctx, ext_name, timeout=15):
    deadline = asyncio.get_event_loop().time() + timeout

    while asyncio.get_event_loop().time() < deadline:
        for page in ctx.background_pages:
            if ext_name in page.url.lower() or "chrome-extension://" in page.url:
                return page

        workers = ctx.service_workers if hasattr(ctx, "service_workers") else []
        for sw in workers:
            if "chrome-extension://" in sw.url:
                return sw

        await asyncio.sleep(0.5)

    if ctx.background_pages:
        return ctx.background_pages[0]
    workers = ctx.service_workers if hasattr(ctx, "service_workers") else []
    if workers:
        return workers[0]
    return None


async def _activate_browsec(ctx, ext_name):
    for page in ctx.pages[1:]:
        try:
            await page.close()
        except Exception:
            pass


async def _wait_for_pac(bg, ext_name, timeout=15):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            has_pac = await bg.evaluate(
                """async () => {
                const items = await new Promise(r => chrome.storage.local.get('lowLevelPac', r));
                const pac = items['lowLevelPac'];
                return !!(pac && pac.countries && Object.keys(pac.countries).length > 0);
            }"""
            )
            if has_pac:
                return True
        except Exception:
            pass
        await asyncio.sleep(1)
    log.warning(f"[VPN] {ext_name}: PAC data not loaded after {timeout}s")
    return False


async def get_available_countries(bg, ext_name):
    try:
        countries = await bg.evaluate(
            """async () => {
            const items = await new Promise(r => chrome.storage.local.get('lowLevelPac', r));
            const pac = items['lowLevelPac'];
            if (!pac || !pac.countries) return [];
            return Object.keys(pac.countries);
        }"""
        )
        return countries or []
    except Exception as e:
        log.warning(f"[VPN] {ext_name}: failed to read countries: {e}")
        return []


async def launch_vpn_context(
    pw, ext_name, server_id, identity=None, headless=False, cfg_path=None
):
    identity = identity or SessionIdentity()

    ext_cfg = EXTENSIONS.get(ext_name)
    if not ext_cfg:
        raise ValueError(f"unknown VPN extension: {ext_name}")

    ext_path = find_extension_path(ext_name, cfg_path=cfg_path)
    if not ext_path:
        raise FileNotFoundError(
            f"extension '{ext_name}' not found in extensions/{ext_name}/ "
            f"(need unpacked CRX with manifest.json)"
        )

    user_data_dir = _make_temp_dir(f"{ext_name}_{server_id}")

    args = list(CHROMIUM_ARGS_VPN) + [
        f"--disable-extensions-except={ext_path}",
        f"--load-extension={ext_path}",
    ]

    vp = {"width": 800, "height": 600} if headless else identity.viewport
    ctx = await pw.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        headless=headless,
        args=args,
        user_agent=identity.user_agent,
        viewport=vp,
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        color_scheme="light",
        device_scale_factor=identity.scale,
    )
    await _stealth.apply_stealth_async(ctx)

    await asyncio.sleep(2)
    await _activate_browsec(ctx, ext_name)

    bg = await _find_ext_worker(ctx, ext_name)
    if not bg:
        log.warning(
            f"[VPN] {ext_name}: background page not found, extension may not work"
        )
        return ctx, None

    if not ext_cfg.get("connect_js"):
        log.warning(f"[VPN] {ext_name}: no connect_js, extension needs manual research")
        return ctx, bg

    try:
        await bg.evaluate(
            """async () => {
            await new Promise(r => chrome.storage.local.set({
                'agreed': true, 'termsAccepted': true,
                'onboardingCompleted': true, 'consent': true
            }, r));
        }"""
        )
    except Exception:
        pass

    await _wait_for_pac(bg, ext_name, timeout=15)

    try:
        result = await bg.evaluate(ext_cfg["connect_js"], server_id)
        log.info(f"[VPN] {ext_name}/{server_id}: connected via {result}")
    except Exception as e:
        log.warning(f"[VPN] {ext_name}/{server_id}: connect failed: {e}")

    await asyncio.sleep(3)

    ip = await check_vpn_ip(ctx)
    if ip:
        log.info(f"[VPN] {ext_name}/{server_id}: IP = {ip}")
    else:
        log.warning(f"[VPN] {ext_name}/{server_id}: could not verify IP")

    return ctx, bg


async def discover_vpn_servers(pw, ext_name, cfg_path=None):
    identity = SessionIdentity()
    ext_cfg = EXTENSIONS.get(ext_name)
    if not ext_cfg:
        return []

    ext_path = find_extension_path(ext_name, cfg_path=cfg_path)
    if not ext_path:
        return []

    user_data_dir = _make_temp_dir(f"{ext_name}_discover")
    args = list(CHROMIUM_ARGS_VPN) + [
        f"--disable-extensions-except={ext_path}",
        f"--load-extension={ext_path}",
    ]

    ctx = None
    try:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            args=args,
            user_agent=identity.user_agent,
            viewport=identity.viewport,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        await _stealth.apply_stealth_async(ctx)

        await asyncio.sleep(2)
        await _activate_browsec(ctx, ext_name)

        bg = await _find_ext_worker(ctx, ext_name)
        countries = []
        if bg:
            await _wait_for_pac(bg, ext_name, timeout=15)
            countries = await get_available_countries(bg, ext_name)
            log.info(
                f"[VPN] {ext_name}: {len(countries)} countries available: {countries}"
            )

        return countries
    except Exception as e:
        log.warning(f"[VPN] {ext_name}: discover failed: {e}")
        return []
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass


async def switch_vpn_server(bg, ext_name, server_id):
    ext_cfg = EXTENSIONS.get(ext_name)
    if not ext_cfg:
        raise ValueError(f"unknown VPN extension: {ext_name}")
    result = await bg.evaluate(ext_cfg["connect_js"], server_id)
    await asyncio.sleep(2)
    return result


async def check_vpn_ip(ctx, timeout=10000):
    page = await ctx.new_page()
    try:
        await page.goto("https://api.ipify.org", timeout=timeout)
        text = (await page.inner_text("body")).strip()
        if text and "." in text:
            return text
    except Exception as e:
        log.debug(f"[VPN] ip check via ipify failed: {e}")
    try:
        await page.goto("https://ifconfig.me/ip", timeout=timeout)
        text = (await page.inner_text("body")).strip()
        if text and "." in text:
            return text
    except Exception as e:
        log.debug(f"[VPN] ip check via ifconfig failed: {e}")
    finally:
        try:
            await page.close()
        except Exception:
            pass
    return None
