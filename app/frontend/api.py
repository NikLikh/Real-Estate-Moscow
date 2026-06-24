import os

import requests

BASE_URL = os.getenv("BACKEND_URL", "http://localhost:8000")


def get_json(path, params=None):
    r = requests.get(BASE_URL + path, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def download_export():
    r = requests.get(BASE_URL + "/listings/current/export", timeout=300)
    r.raise_for_status()
    return r.content
