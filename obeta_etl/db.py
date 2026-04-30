from __future__ import annotations

from typing import Sequence

import mysql.connector

from obeta_etl.config import DBConfig


def connect(cfg: DBConfig):
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        autocommit=False,
        allow_local_infile=True,
    )


def qname(db: str, table: str) -> str:
    """Backtick-quote a database.table name for MySQL."""
    return f"`{db}`.`{table}`"


def col_list(columns: Sequence[str]) -> str:
    return ", ".join(f"`{c}`" for c in columns)


def placeholders(n: int) -> str:
    return ", ".join(["%s"] * n)


def execute(conn, sql: str, params: Sequence | None = None, commit: bool = False) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())

    if commit:
        conn.commit()


def fetch_one(conn, sql: str, params: Sequence | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def ensure_databases(conn, cfg: DBConfig) -> None:
    execute(conn, f"CREATE DATABASE IF NOT EXISTS `{cfg.staging_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    execute(conn, f"CREATE DATABASE IF NOT EXISTS `{cfg.production_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    conn.commit()


def truncate_or_drop(conn, db: str, table: str, drop: bool = False) -> None:
    if drop:
        execute(conn, f"DROP TABLE IF EXISTS {qname(db, table)}")
    else:
        execute(conn, f"TRUNCATE TABLE {qname(db, table)}")