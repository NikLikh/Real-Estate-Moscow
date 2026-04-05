import asyncio
import logging
import random
import time
from urllib.parse import urlparse
from typing import Sequence

from patchright.async_api import Browser, BrowserContext, Frame, Page
from playwright_stealth import Stealth

log = logging.getLogger("re")


class BrowserPool:
    # пул из N Chromium, round-robin

    def __init__(self, browsers: list[Browser]):
        self._browsers = browsers
        self._index = 0

    def get(self) -> Browser:
        browser = self._browsers[self._index % len(self._browsers)]
        self._index += 1
        return browser

    @property
    def all(self) -> list[Browser]:
        return list(self._browsers)

    async def close_all(self):
        for b in self._browsers:
            try:
                await b.close()
            except Exception:
                pass


async def launch_browser_pool(playwright, n: int, headless=False) -> BrowserPool:
    browsers = []
    for i in range(n):
        b = await launch_stealth_browser(playwright, headless=headless)
        browsers.append(b)
        log.info(f"browser {i + 1}/{n} launched")
    return BrowserPool(browsers)


# Chrome UA
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
    # фикс fingerprint на время жизни воркера, иначе WAF палит ротацию

    def __init__(self, user_agent=None, viewport=None, scale=None):
        self.user_agent = user_agent or random.choice(USER_AGENTS)
        self.viewport = dict(viewport or random.choice(VIEWPORTS))
        self.scale = scale if scale is not None else random.choice([1, 1.25, 1.5])

    def to_dict(self):
        return {
            "user_agent": self.user_agent,
            "viewport": dict(self.viewport),
            "scale": self.scale,
        }

    @classmethod
    def from_dict(cls, payload):
        if not payload:
            return cls()
        return cls(
            user_agent=payload.get("user_agent"),
            viewport=payload.get("viewport"),
            scale=payload.get("scale"),
        )


# блокируем трекеры, карты, шрифты, медиа чтобы экономить RAM и трафик
BLOCKED_PATTERNS = [
    "**/mc.yandex.ru/**",
    "**/google-analytics.com/**",
    "**/googletagmanager.com/**",
    "**/doubleclick.net/**",
    "**/facebook.net/**",
    "**/top-fwz1.mail.ru/**",
    "**/vk.com/rtrg**",
    "**/an.yandex.ru/**",
    "**/ads.adfox.ru/**",
    "**/wcm.weborama-tech.ru/**",
    "**/ad.mail.ru/**",
    "**/counter.yadro.ru/**",
    "**/maps.googleapis.com/**",
    "**/maps.gstatic.com/**",
    "**/api-maps.yandex.ru/**",
    "**/suggest-maps.yandex.ru/**",
    "**/maps.api.2gis.ru/**",
    "**/*.woff2",
    "**/*.woff",
    "**/*.mp4",
    "**/*.webm",
    "**/cdn-p.cian.site/**",
]

# для offer-страниц: JS, CSS, SVG, картинки
# JS не нужен т.к. все данные в SSR (data-testid, data-name, inline <script>)
# goto 0.5-1с вместо 20-115с
OFFER_EXTRA_BLOCKED = [
    "**/*.js",
    "**/*.css",
    "**/*.svg",
    "**/*.jpg",
    "**/*.jpeg",
    "**/*.png",
    "**/*.webp",
    "**/*.gif",
]

CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-component-extensions-with-background-pages",
    "--blink-settings=imagesEnabled=false",  # дублирует BLOCKED_PATTERNS, но срезает до сети
    "--disable-http2",  # HTTP/2 keep-alive вызывает ERR_HTTP2_PING_FAILED
    "--no-proxy-server",  # игнорировать системный прокси (Happ/v2ray ставит system proxy)
    # экономия RAM и CPU -- не нужны для парсинга
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

# для VPN-расширений: без --disable-component-extensions (убивает background page расширения)
# и без --no-proxy-server (расширение маршрутизирует через chrome.proxy API)
CHROMIUM_ARGS_VPN = [
    a
    for a in CHROMIUM_ARGS
    if a
    not in ("--disable-component-extensions-with-background-pages", "--no-proxy-server")
]

_stealth = Stealth(
    navigator_languages_override=["ru-RU", "ru", "en-US", "en"],
)

# domrf: ServicePipe детектит --disable-http2, --disable-gpu, playwright_stealth
# headed hidden (--window-position) вместо headless
CHROMIUM_ARGS_DOMRF = [
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--window-position=-32000,-32000",
    "--disable-dev-shm-usage",
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


async def launch_domrf_browser(playwright) -> Browser:
    return await playwright.chromium.launch(
        headless=False,
        args=CHROMIUM_ARGS_DOMRF,
    )


async def launch_stealth_browser(playwright, headless=False) -> Browser:
    return await playwright.chromium.launch(
        headless=headless,
        args=CHROMIUM_ARGS,
    )


async def create_stealth_context(
    browser: Browser,
    proxy=None,
    identity: SessionIdentity = None,
    user_agent=None,
    viewport=None,
    storage_state=None,
) -> BrowserContext:
    ua = identity.user_agent if identity else (user_agent or random.choice(USER_AGENTS))
    vp = identity.viewport if identity else (viewport or random.choice(VIEWPORTS))
    scale = identity.scale if identity else random.choice([1, 1.25, 1.5])

    ctx_kwargs = dict(
        user_agent=ua,
        viewport=vp,
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        color_scheme="light",
        device_scale_factor=scale,
    )
    if proxy:
        ctx_kwargs["proxy"] = proxy
    if storage_state:
        ctx_kwargs["storage_state"] = storage_state

    context = await browser.new_context(**ctx_kwargs)
    await _stealth.apply_stealth_async(context)
    return context


async def apply_cdp_blocking(page: Page, extra_patterns: Sequence[str] = ()):
    """блокировка ресурсов через CDP (быстрее чем route.abort)"""
    patterns = list(BLOCKED_PATTERNS) + list(extra_patterns)
    try:
        client = await page.context.new_cdp_session(page)
        await client.send("Network.setBlockedURLs", {"urls": patterns})
        await client.send("Network.enable")
        return client
    except Exception as e:
        log.warning(f"CDP blocking failed ({e}), falling back to route.abort()")
        for pattern in patterns:
            await page.route(pattern, lambda route: route.abort())
        return None


async def reset_cdp_blocking(client):
    try:
        await client.send("Network.setBlockedURLs", {"urls": []})
    except Exception:
        pass


async def humanize(page: Page):
    # имитация живого человека, двигаем мышь и скроллим
    vp = page.viewport_size or {"width": 1920, "height": 1080}

    for _ in range(random.randint(3, 5)):
        x = random.randint(100, vp["width"] - 100)
        y = random.randint(100, vp["height"] - 100)
        await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.4))

    await page.mouse.wheel(0, random.randint(200, 600))
    await asyncio.sleep(random.uniform(0.5, 1.5))

    if random.random() < 0.2:
        await page.mouse.wheel(0, -random.randint(100, 300))
        await asyncio.sleep(random.uniform(0.3, 0.8))


async def jittered_delay(min_s: float, max_s: float):
    # gaussian реалистичнее чем uniform
    mean = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, min(max_s, random.gauss(mean, std)))
    await asyncio.sleep(delay)


async def warmup_session(page: Page):
    # визит на главную создает cookies и историю
    # JS должен быть разрешен на этом этапе -- WAF-скрипты ставят cookies
    try:
        await page.goto(
            "https://www.cian.ru/", timeout=30000, wait_until="domcontentloaded"
        )
        await jittered_delay(3.0, 5.0)
        await humanize(page)
        # ждём networkidle -- WAF-скрипты грузятся асинхронно
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
    except Exception:
        pass


async def _pick_first(page: Page, selector: str):
    locator = page.locator(selector).first
    if await locator.count() == 0:
        return None
    return locator


async def dismiss_web_proxy_consent(page: Page):
    selectors = [
        "button[aria-label*='Accept']",
        "button[title*='Accept']",
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
        "button:has-text('Consent')",
        "[aria-label='Consent'] button",
        ".fc-cta-consent",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            await locator.click(timeout=3000)
            await asyncio.sleep(1)
            return
        except Exception:
            continue


async def _find_web_proxy_frame(page: Page, target_url: str | None = None):
    target_host = ""
    if target_url:
        try:
            target_host = urlparse(target_url).netloc.lower()
        except Exception:
            target_host = ""

    for frame in page.frames:
        if frame == page.main_frame:
            continue
        if target_host and target_host in frame.url.lower():
            return frame

    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            html = (await frame.content()).lower()
        except Exception:
            continue
        if any(
            mark in html for mark in ("cian", "циан", "price-amount", "cardcomponent")
        ):
            return frame

    return None


async def submit_web_proxy_target(page: Page, gateway_cfg: dict, target_url: str):
    await page.goto(
        gateway_cfg["landing_url"], timeout=30000, wait_until="domcontentloaded"
    )
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    await dismiss_web_proxy_consent(page)

    input_box = await _pick_first(page, gateway_cfg["url_input_selector"])
    if not input_box:
        raise RuntimeError(f"url input not found: {gateway_cfg['url_input_selector']}")

    await input_box.fill(target_url)

    submit = await _pick_first(page, gateway_cfg["submit_selector"])
    if submit:
        await submit.click()
    else:
        await input_box.press("Enter")

    await asyncio.sleep(gateway_cfg.get("settle_delay", 8))
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
    except Exception:
        pass

    frame = await _find_web_proxy_frame(page, target_url)
    return frame


class WebProxyPageAdapter:

    def __init__(self, page: Page, gateway_cfg: dict):
        self._page = page
        self._cfg = gateway_cfg
        self._frame = None
        self._target_url = gateway_cfg.get("target_url")

    @property
    def context(self):
        return self._page.context

    @property
    def url(self):
        if self._frame and self._frame.url:
            return self._frame.url
        return self._page.url

    @property
    def frames(self):
        return self._page.frames

    @property
    def mouse(self):
        return self._page.mouse

    @property
    def viewport_size(self):
        return self._page.viewport_size

    async def goto(self, url, timeout=30000, wait_until="domcontentloaded"):
        del timeout, wait_until
        self._target_url = url
        self._frame = await submit_web_proxy_target(self._page, self._cfg, url)
        return None

    async def title(self):
        if self._frame:
            try:
                return await self._frame.title()
            except Exception:
                pass
        return await self._page.title()

    async def content(self):
        target = self._frame or self._page
        return await target.content()

    async def query_selector(self, selector):
        target = self._frame or self._page
        return await target.query_selector(selector)

    async def query_selector_all(self, selector):
        target = self._frame or self._page
        return await target.query_selector_all(selector)

    async def wait_for_selector(self, selector, timeout=None):
        target = self._frame or self._page
        return await target.wait_for_selector(selector, timeout=timeout)

    async def reload(self, timeout=30000):
        await self._page.reload(timeout=timeout)
        if self._target_url:
            self._frame = await _find_web_proxy_frame(self._page, self._target_url)

    async def wait_for_load_state(self, state="load", timeout=None):
        return await self._page.wait_for_load_state(state, timeout=timeout)

    async def wait_for_url(self, url, timeout=None):
        return await self._page.wait_for_url(url, timeout=timeout)


# ищем в HTML, если находим значит нарвались на капчу
CAPTCHA_SIGNS = [
    "smartcaptcha",
    "captcha-api.yandex",
    "не робот",
    "CheckBrowser",
    "datadome",
    "showcaptcha",
    "cian-captcha",
]

VPN_SIGNS = [
    "похоже, у вас включен vpn",
    "кажется, у вас включён vpn",
    "кажется, у вас включен vpn",
    "включен vpn",
    "включён vpn",
    "vpn или прокси",
    "vpn detected",
]

# WAF rate limit отдельно от VPN-блока, при нём нужна ротация endpoint
WAF_RATE_LIMIT_SIGNS = [
    "cian_waf_block",
    "cian_waf_rate_limit",
]


async def _get_page_text(page: Page) -> str:
    try:
        content = await page.content()
        title = await page.title()
        return (content + " " + title).lower()
    except Exception:
        return ""


async def detect_captcha(page: Page, url_only=False) -> bool:
    # url_only=True когда JS заблокирован (иначе false-positive от <script> тегов)
    try:
        url = page.url.lower()
        if "captcha" in url or "showcaptcha" in url:
            return True
    except Exception:
        pass

    if url_only:
        return False

    text = await _get_page_text(page)
    if not text:
        return False
    return any(sign.lower() in text for sign in CAPTCHA_SIGNS)


async def detect_vpn_block(page: Page) -> bool:
    text = await _get_page_text(page)
    return any(sign in text for sign in VPN_SIGNS)


async def detect_waf_rate_limit(page: Page) -> bool:
    text = await _get_page_text(page)
    return any(sign in text for sign in WAF_RATE_LIMIT_SIGNS)


async def handle_vpn_block(page: Page, max_retries=3) -> bool:
    for attempt in range(max_retries):
        if not await detect_vpn_block(page):
            return True

        log.info(f"    vpn block, refreshing ({attempt + 1})...")
        await jittered_delay(3.0, 6.0)
        try:
            await page.reload(timeout=30000)
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            await jittered_delay(5.0, 10.0)
        await jittered_delay(2.0, 4.0)

    still_blocked = await detect_vpn_block(page)
    if still_blocked:
        log.warning("    vpn block persists, skipping")
    return not still_blocked


CAPTCHA_BUTTON_SELECTORS = [
    "#js-button",
    ".CheckboxCaptcha-Button",
    'input[type="submit"]',
    "button.CaptchaButton",
    ".smartcaptcha button",
    "#checkbox-button",
    'button[data-testid="checkbox-button"]',
]


async def _try_click_element(page: Page, selectors: list[str], frame=None) -> bool:
    target = frame or page
    for selector in selectors:
        btn = await target.query_selector(selector)
        if not btn:
            continue
        # на page двигаем мышь к кнопке (реалистичнее), на frame просто btn.click()
        if frame is None:
            box = await btn.bounding_box()
            if box and box["width"] > 0 and box["height"] > 0:
                x = box["x"] + random.uniform(5, box["width"] - 5)
                y = box["y"] + random.uniform(5, box["height"] - 5)
                await page.mouse.move(x, y, steps=random.randint(10, 25))
                await asyncio.sleep(random.uniform(0.3, 0.8))
                await page.mouse.click(x, y)
                return True
        try:
            await btn.click()
            return True
        except Exception:
            continue
    return False


async def _try_click_smartcaptcha(page: Page) -> bool:
    if await _try_click_element(page, CAPTCHA_BUTTON_SELECTORS):
        return True

    for frame in page.frames:
        url = frame.url.lower()
        if "smartcaptcha" in url or "captcha-api" in url or "captcha" in url:
            if await _try_click_element(page, CAPTCHA_BUTTON_SELECTORS, frame=frame):
                return True

    return False


# селекторы галочки (checked state) SmartCaptcha
CAPTCHA_CHECKED_SELECTORS = [
    ".CheckboxCaptcha-Checkbox_checked",
    ".CheckboxCaptcha_checked",
    'input[type="checkbox"]:checked',
    ".smartcaptcha .checked",
    '[data-checked="true"]',
]


async def _is_checkbox_checked(page: Page) -> bool:
    for sel in CAPTCHA_CHECKED_SELECTORS:
        el = await page.query_selector(sel)
        if el:
            return True

    # иногда галочка это SVG внутри кнопки
    for sel in CAPTCHA_BUTTON_SELECTORS:
        btn = await page.query_selector(sel)
        if not btn:
            continue
        # svg/path внутри кнопки = галочка
        svg = await btn.query_selector("svg")
        if svg:
            return True

    # проверяем во фреймах
    for frame in page.frames:
        if frame.url == page.url:
            continue
        url = frame.url.lower()
        if "captcha" not in url and "smartcaptcha" not in url:
            continue
        for sel in CAPTCHA_CHECKED_SELECTORS:
            el = await frame.query_selector(sel)
            if el:
                return True

    return False


async def handle_captcha(page: Page, max_attempts=3, url_only=False) -> bool:
    if not await detect_captcha(page, url_only=url_only):
        return True

    log.info("    CAPTCHA, trying auto-click...")

    for attempt in range(max_attempts):
        # SmartCaptcha анализирует timing, поэтому ждем подольше
        await jittered_delay(2.5, 5.0)

        clicked = await _try_click_smartcaptcha(page)
        if clicked:
            log.info(f"    clicked checkbox ({attempt + 1})")

            # галочка это главный флаг решения капчи
            await asyncio.sleep(1.5)
            if await _is_checkbox_checked(page):
                log.info("    checkbox checked, waiting for redirect...")
                # галочка есть, ждем пока страница перезагрузится
                try:
                    await page.wait_for_url(
                        lambda url: "captcha" not in url.lower(), timeout=15000
                    )
                except Exception:
                    pass
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass
                await jittered_delay(1.0, 2.0)
                log.info("    captcha solved (checkbox checked)")
                return True

            # redirect без галочки (бывает на старых версиях капчи)
            try:
                await page.wait_for_url(
                    lambda url: "captcha" not in url.lower(), timeout=8000
                )
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                await jittered_delay(1.0, 2.0)
                log.info("    captcha solved (redirect)")
                return True
            except Exception:
                pass

            await jittered_delay(2.0, 4.0)
            if not await detect_captcha(page):
                log.info("    captcha solved")
                return True
            else:
                log.info("    captcha still present")
        else:
            frame_urls = [f.url for f in page.frames if f.url != page.url]
            log.info(
                f"    checkbox not found ({attempt + 1}) "
                f"url={page.url[:60]} frames={frame_urls[:3]}"
            )
            await jittered_delay(2.0, 4.0)

    # длинная пауза, иногда капча уходит сама
    log.info("    captcha not solved, long pause before giving up...")
    await jittered_delay(15.0, 30.0)

    # после паузы проверяем и галочку, и исчезновение капчи
    if await _is_checkbox_checked(page):
        log.info("    checkbox checked after pause")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass
        return True

    if not await detect_captcha(page):
        log.info("    captcha gone after pause")
        return True

    try:
        await page.reload(timeout=15000)
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        await jittered_delay(3.0, 5.0)
        if not await detect_captcha(page):
            log.info("    captcha gone after reload")
            return True
    except Exception:
        pass

    log.warning("    captcha not solved, skipping")
    return False


class AdaptiveDelay:
    # увеличивает задержки после капчи, снижает после серии успехов

    def __init__(
        self,
        base_range=(0.5, 1.5),
        heated_range=(2.0, 4.0),
        cooldown_after=6,
    ):
        self.base_range = base_range
        self.heated_range = heated_range
        self.cooldown_after = cooldown_after
        self._heated = False
        self._streak = 0
        self._active_captchas = 0

    def report_captcha(self):
        self._heated = True
        self._streak = 0

    def report_success(self):
        self._streak += 1
        if self._streak >= self.cooldown_after:
            self._heated = False

    def captcha_enter(self):
        self._active_captchas += 1

    def captcha_exit(self):
        self._active_captchas = max(0, self._active_captchas - 1)

    @property
    def under_pressure(self) -> bool:
        return self._active_captchas >= 2

    async def wait(self):
        r = self.heated_range if self._heated else self.base_range
        await jittered_delay(*r)

    @property
    def is_heated(self) -> bool:
        return self._heated
