from __future__ import annotations

from obeta_etl.db import execute, fetch_one, qname
from obeta_etl.logger import log


def index_exists(conn, db: str, table: str, index_name: str) -> bool:
    row = fetch_one(
        conn,
        """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = %s
          AND table_name = %s
          AND index_name = %s
        """,
        (db, table, index_name),
    )
    return bool(row and row[0])


def add_index_if_missing(conn, db: str, table: str, index_name: str, expression: str) -> None:
    if not index_exists(conn, db, table, index_name):
        log(f"Creating index {index_name} on {db}.{table}")
        execute(conn, f"ALTER TABLE {qname(db, table)} ADD INDEX `{index_name}` ({expression})")