
MAX_BODY_BYTES = 5_000_000
SCAN_CHARS = 32_768

WAF_RATE_LIMIT_SIGNS = ["cian_waf_block", "cian_waf_rate_limit"]

CAPTCHA_SIGNS = [
    "smartcaptcha", "captcha-api.yandex", "не робот",
    "вы не робот",
]

VPN_SIGNS = [
    "похоже, у вас включен vpn", "кажется, у вас включён vpn",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def headers():
    return {
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.cian.ru/",
        "Cache-Control": "no-cache",
    }


def is_waf(html: str, status: int) -> bool:
    if status in (403, 429, 503):
        return True
    low = html[:SCAN_CHARS].lower()
    return any(sign in low for sign in WAF_RATE_LIMIT_SIGNS)


def is_captcha(html: str, url: str = "") -> bool:
    url_low = url.lower()
    if "captcha" in url_low or "showcaptcha" in url_low:
        return True
    low = html[:SCAN_CHARS].lower()
    return any(sign.lower() in low for sign in CAPTCHA_SIGNS)


def is_vpn_block(html: str) -> bool:
    low = html[:SCAN_CHARS].lower()
    return any(sign.lower() in low for sign in VPN_SIGNS)
