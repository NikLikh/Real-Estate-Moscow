"""идемпотентный раннер схемы

применяет db/schema/**/*.sql в порядке числовых префиксов имён. все DDL
используют IF NOT EXISTS, поэтому повторный прогон безопасен.

запуск:
    python -m db.apply
    python -m db.apply --dry-run
"""
import argparse
import logging
from pathlib import Path

from db.connection import get_conn, put_conn

log = logging.getLogger("re.apply")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

SCHEMA_DIR = Path(__file__).parent / "schema"


def _schema_files():
    # сортировка по basename: числовой префикс задаёт порядок слоёв
    if not SCHEMA_DIR.exists():
        return []
    return sorted(SCHEMA_DIR.rglob("*.sql"), key=lambda p: p.name)


def apply(dry_run=False):
    conn = get_conn()
    try:
        cur = conn.cursor()
        files = _schema_files()
        log.info(f"schema files: {len(files)}")
        for f in files:
            rel = f.relative_to(SCHEMA_DIR)
            log.info(f"  schema: {rel}")
            if dry_run:
                continue
            sql = f.read_text(encoding="utf-8")
            if sql.strip():
                cur.execute(sql)
        if not dry_run:
            conn.commit()
        cur.close()
        log.info("apply complete")
    except Exception as e:
        conn.rollback()
        log.error(f"apply failed: {e}")
        raise
    finally:
        put_conn(conn)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="show what would be applied")
    args = ap.parse_args()
    apply(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
