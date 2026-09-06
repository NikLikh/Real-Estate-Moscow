import os
import sys

import pandas as pd
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import DB_CONFIG

_engine = None
_pool = None


def get_engine():
    global _engine
    if _engine is None:
        c = DB_CONFIG
        _engine = create_engine(
            f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['dbname']}",
            pool_pre_ping=True,
        )
    return _engine


def _get_pool():
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(1, 8, **DB_CONFIG)
    return _pool


def fetch_all(sql, params=()):
    p = _get_pool()
    conn = p.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.rollback()
        p.putconn(conn)


def fetch_df(sql):
    return pd.read_sql(sql, get_engine())
