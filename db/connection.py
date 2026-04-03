# пул соединений к постгресу, один на процесс, создается лениво
from psycopg2.pool import ThreadedConnectionPool

from config.settings import DB_CONFIG

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(2, 8, **DB_CONFIG)  # min=2, max=8
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn):
    _get_pool().putconn(conn)
