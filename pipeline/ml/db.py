import os
import sys

from sqlalchemy import create_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import DB_CONFIG


def make_engine():
    c = DB_CONFIG
    return create_engine(
        f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['dbname']}"
    )
