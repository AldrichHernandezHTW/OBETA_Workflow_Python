#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import mysql.connector
import pandas as pd

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is optional
    load_dotenv = None


# -----------------------------
# Configuration
# -----------------------------


@dataclass(frozen=True)
class DBConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    staging_db: str = "obeta_staging"
    production_db: str = "obeta_production"

    @staticmethod
    def from_env() -> "DBConfig":
        if load_dotenv:
            load_dotenv()
        return DBConfig(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", ""),
            staging_db=os.getenv("STAGING_DB", "obeta_staging"),
            production_db=os.getenv("PRODUCTION_DB", "obeta_production"),
        )


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


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


# -----------------------------
# DDL
# -----------------------------


def create_staging_tables(conn, cfg: DBConfig, reset_output: bool = False) -> None:
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.staging_db, 'warehouse_sections')} (
            warehouse_abbreviation VARCHAR(50),
            warehouse_description VARCHAR(255),
            warehouse_section VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.staging_db, 'product_data')} (
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.staging_db, 'pick_data')} (
            product_id VARCHAR(50),
            warehouse_section VARCHAR(50),
            origin INT,
            order_id BIGINT,
            position_in_order INT,
            pick_volume INT,
            quantity_unit VARCHAR(10),
            `date` DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    if reset_output:
        for table in ["warehouse_sections", "product_data", "pick_data"]:
            log(f"Truncating staging table: {table}")
            truncate_or_drop(conn, cfg.staging_db, table, drop=False)

    conn.commit()


def create_pickoperations_table(conn, cfg: DBConfig, reset_output: bool = False) -> None:
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.staging_db, 'pickoperations')} (
            pick_id VARCHAR(50),
            order_id VARCHAR(50),
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20),
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12, 6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    if reset_output:
        log("Truncating staging table: pickoperations")
        truncate_or_drop(conn, cfg.staging_db, "pickoperations", drop=False)
    conn.commit()


def create_production_tables(conn, cfg: DBConfig, reset_output: bool = False) -> None:
    if reset_output:
        log("Dropping production tables in FK-safe order")
        execute(conn, "SET FOREIGN_KEY_CHECKS = 0")
        for table in ["pick", "order", "product"]:
            execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, table)}")
        execute(conn, "SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()

    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'product')} (
            product_key INT AUTO_INCREMENT PRIMARY KEY,
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50),
            INDEX idx_product_id (product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'order')} (
            order_key INT AUTO_INCREMENT PRIMARY KEY,
            order_id VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20),
            INDEX idx_order_id (order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'pick')} (
            pick_id VARCHAR(50),
            product_key INT,
            order_key INT,
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12,6),
            INDEX idx_pick_product_key (product_key),
            INDEX idx_pick_order_key (order_key),
            CONSTRAINT fk_pick_product FOREIGN KEY (product_key)
                REFERENCES {qname(cfg.production_db, 'product')}(product_key),
            CONSTRAINT fk_pick_order FOREIGN KEY (order_key)
                REFERENCES {qname(cfg.production_db, 'order')}(order_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    conn.commit()


# -----------------------------
# Batched DataFrame -> MySQL writer
# -----------------------------


def clean_dataframe_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    """Convert NaN/NaT to None so mysql-connector can insert them."""
    return df.astype(object).where(pd.notnull(df), None)


def batch_insert_dataframe(conn, db: str, table: str, df: pd.DataFrame, batch_size: int) -> int:
    if df.empty:
        return 0

    columns = list(df.columns)
    sql = f"INSERT INTO {qname(db, table)} ({col_list(columns)}) VALUES ({placeholders(len(columns))})"
    df = clean_dataframe_for_mysql(df)

    inserted = 0
    with conn.cursor() as cur:
        rows = list(df.itertuples(index=False, name=None))
        for start in range(0, len(rows), batch_size):
            cur.executemany(sql, rows[start:start + batch_size])
            inserted += cur.rowcount if cur.rowcount != -1 else len(rows[start:start + batch_size])
    conn.commit()
    return inserted


# -----------------------------
# Flow 1: Raw CSV -> Staging
# -----------------------------


def flow1_raw_to_staging(
    conn,
    cfg: DBConfig,
    warehouse_csv: Path,
    product_csv: Path,
    pick_csv: Path,
    csv_chunk_size: int,
    db_batch_size: int,
    reset_output: bool,
    product_encoding: str = "cp1252",
) -> None:
    log("START Flow 1: Raw CSV files -> OBETA_STAGING")
    ensure_databases(conn, cfg)
    create_staging_tables(conn, cfg, reset_output=reset_output)

    # 001 warehouse_section.csv
    log("Loading warehouse_sections")
    wh_cols = ["warehouse_abbreviation", "warehouse_description", "warehouse_section"]
    wh_reader = pd.read_csv(
        warehouse_csv,
        sep=";",
        header=0,
        encoding="utf-8",
        chunksize=csv_chunk_size,
        dtype=str,
    )
    total = 0
    for chunk in wh_reader:
        chunk = chunk.rename(columns={
            "Abkürzung": "warehouse_abbreviation",
            "Bedeutung": "warehouse_description",
            "Gruppe": "warehouse_section",
        })
        chunk = chunk[[c for c in wh_cols if c in chunk.columns]]
        missing = [c for c in wh_cols if c not in chunk.columns]
        if missing:
            raise ValueError(f"warehouse_sections CSV is missing columns after rename: {missing}")
        total += batch_insert_dataframe(conn, cfg.staging_db, "warehouse_sections", chunk[wh_cols], db_batch_size)
    log(f"warehouse_sections inserted rows: {total:,}")

    # 002 product_data.csv - no header in the Tableau flow
    log("Loading product_data")
    product_cols = ["product_id", "product_description", "product_group"]
    product_reader = pd.read_csv(
        product_csv,
        sep=",",
        header=None,
        names=product_cols,
        encoding=product_encoding,
        chunksize=csv_chunk_size,
        dtype=str,
    )
    total = 0
    for chunk in product_reader:
        chunk = chunk[product_cols]
        total += batch_insert_dataframe(conn, cfg.staging_db, "product_data", chunk, db_batch_size)
    log(f"product_data inserted rows: {total:,}")

    # 003 pick_data.csv - no header in the Tableau flow
    log("Loading pick_data")
    pick_cols = [
        "product_id", "warehouse_section", "origin", "order_id", "position_in_order",
        "pick_volume", "quantity_unit", "date"
    ]
    pick_reader = pd.read_csv(
        pick_csv,
        sep=",",
        header=None,
        names=pick_cols,
        encoding="utf-8",
        chunksize=csv_chunk_size,
        dtype={
            "product_id": "string",
            "warehouse_section": "string",
            "quantity_unit": "string",
        },
    )
    total = 0
    for chunk in pick_reader:
        for c in ["origin", "order_id", "position_in_order", "pick_volume"]:
            chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
        chunk = chunk[pick_cols]
        total += batch_insert_dataframe(conn, cfg.staging_db, "pick_data", chunk, db_batch_size)
    log(f"pick_data inserted rows: {total:,}")

    log("Creating staging indexes for Flow 2")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_order_date", "order_id, `date`")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_warehouse", "warehouse_section")
    add_index_if_missing(conn, cfg.staging_db, "product_data", "idx_product_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "warehouse_sections", "idx_warehouse_abbreviation", "warehouse_abbreviation")
    conn.commit()
    log("END Flow 1")


# -----------------------------
# Index helpers
# -----------------------------


def index_exists(conn, db: str, table: str, index_name: str) -> bool:
    row = fetch_one(conn, """
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = %s
          AND table_name = %s
          AND index_name = %s
    """, (db, table, index_name))
    return bool(row and row[0])


def add_index_if_missing(conn, db: str, table: str, index_name: str, expression: str) -> None:
    if not index_exists(conn, db, table, index_name):
        log(f"Creating index {index_name} on {db}.{table}")
        execute(conn, f"ALTER TABLE {qname(db, table)} ADD INDEX `{index_name}` ({expression})")


# -----------------------------
# Flow 2: Staging clean/transform -> pickoperations
# -----------------------------


def flow2_transform_staging(conn, cfg: DBConfig, db_batch_size: int, reset_output: bool) -> None:
    log("START Flow 2: Transform/Clean staging -> pickoperations")
    ensure_databases(conn, cfg)
    execute(conn, f"USE `{cfg.staging_db}`")
    create_pickoperations_table(conn, cfg, reset_output=reset_output)

    # These indexes match the joins in the Tableau flow.
    add_index_if_missing(conn, cfg.staging_db, "pick_data", "idx_pick_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "product_data", "idx_product_data_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "warehouse_sections", "idx_warehouse_abbreviation", "warehouse_abbreviation")
    conn.commit()

    log("Creating temporary duration table: MIN/MAX date by pick_id")
    execute(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_flow2_duration")
    execute(conn, f"""
        CREATE TEMPORARY TABLE tmp_flow2_duration (
            pick_id VARCHAR(50) NOT NULL,
            time_to_fill_minutes DECIMAL(12,6),
            INDEX idx_tmp_flow2_duration_pick_id (pick_id)
        ) ENGINE=InnoDB
    """)
    execute(conn, f"""
        INSERT INTO tmp_flow2_duration (pick_id, time_to_fill_minutes)
        SELECT
            x.pick_id,
            TIMESTAMPDIFF(MINUTE, MIN(x.`date`), MAX(x.`date`)) AS time_to_fill_minutes
        FROM (
            SELECT
                CONCAT(CAST(pd.order_id AS CHAR), CAST(YEAR(pd.`date`) AS CHAR)) AS pick_id,
                pd.`date`
            FROM {qname(cfg.staging_db, 'pick_data')} pd
        ) x
        WHERE x.pick_id IS NOT NULL
        GROUP BY x.pick_id
        HAVING time_to_fill_minutes <= 10080
    """)
    conn.commit()

    log("Creating temporary transformed pickoperations table")
    execute(conn, "DROP TEMPORARY TABLE IF EXISTS tmp_flow2_pickoperations")
    execute(conn, """
        CREATE TEMPORARY TABLE tmp_flow2_pickoperations (
            etl_row_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            pick_id VARCHAR(50),
            order_id VARCHAR(50),
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20),
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12,6),
            INDEX idx_tmp_flow2_row_id (etl_row_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO tmp_flow2_pickoperations (
            pick_id,
            order_id,
            product_id,
            product_description,
            product_group,
            warehouse_section,
            origin,
            position_in_order,
            pick_volume,
            quantity_unit,
            year,
            month,
            quarter,
            order_duration
        )
        SELECT
            p.pick_id,
            CAST(p.order_id AS CHAR) AS order_id,
            p.product_id,
            pr.product_description,
            pr.product_group,
            p.warehouse_section,
            CASE
                WHEN p.origin_num = 46 THEN 'STORE'
                WHEN p.origin_num = 48 THEN 'CUSTOMER'
                ELSE NULL
            END AS origin,
            CAST(p.position_in_order AS CHAR) AS position_in_order,
            p.pick_volume,
            p.quantity_unit,
            p.year,
            p.month,
            p.quarter,
            CAST(COALESCE(d.time_to_fill_minutes, 0) AS DECIMAL(12,6)) AS order_duration
        FROM (
            SELECT
                pd.product_id,
                pd.warehouse_section,
                pd.origin AS origin_num,
                pd.order_id,
                pd.position_in_order,
                pd.pick_volume,
                pd.quantity_unit,
                pd.`date`,
                YEAR(pd.`date`) AS year,
                MONTH(pd.`date`) AS month,
                QUARTER(pd.`date`) AS quarter,
                CONCAT(CAST(pd.order_id AS CHAR), CAST(YEAR(pd.`date`) AS CHAR)) AS pick_id
            FROM {qname(cfg.staging_db, 'pick_data')} pd
            WHERE pd.order_id IS NOT NULL
        ) p
        LEFT JOIN tmp_flow2_duration d
            ON d.pick_id COLLATE utf8mb4_unicode_ci = p.pick_id COLLATE utf8mb4_unicode_ci
        JOIN {qname(cfg.staging_db, 'product_data')} pr
            ON pr.product_id COLLATE utf8mb4_unicode_ci = p.product_id COLLATE utf8mb4_unicode_ci
        LEFT JOIN {qname(cfg.staging_db, 'warehouse_sections')} ws
            ON p.warehouse_section COLLATE utf8mb4_unicode_ci = ws.warehouse_abbreviation COLLATE utf8mb4_unicode_ci
        WHERE p.pick_volume > 0
          AND pr.product_description IS NOT NULL
          AND pr.product_group IS NOT NULL
    """)
    conn.commit()

    max_id = fetch_one(conn, "SELECT COALESCE(MAX(etl_row_id), 0) FROM tmp_flow2_pickoperations")[0]
    log(f"Writing pickoperations in batches. Rows to write: {max_id:,}")

    columns = [
        "pick_id", "order_id", "product_id", "product_description", "product_group",
        "warehouse_section", "origin", "position_in_order", "pick_volume", "quantity_unit",
        "year", "month", "quarter", "order_duration"
    ]
    for start_id in range(1, int(max_id) + 1, db_batch_size):
        end_id = start_id + db_batch_size - 1
        execute(conn, f"""
            INSERT INTO {qname(cfg.staging_db, 'pickoperations')} ({col_list(columns)})
            SELECT {col_list(columns)}
            FROM tmp_flow2_pickoperations
            WHERE etl_row_id BETWEEN %s AND %s
            ORDER BY etl_row_id
        """, (start_id, end_id))
        conn.commit()
        log(f"pickoperations batch committed: {start_id:,} - {min(end_id, int(max_id)):,}")

    log("Creating index on pickoperations for Flow 3")
    add_index_if_missing(conn, cfg.staging_db, "pickoperations", "idx_pickoperations_product", "product_id")
    add_index_if_missing(conn, cfg.staging_db, "pickoperations", "idx_pickoperations_order", "order_id")
    conn.commit()
    log("END Flow 2")


# -----------------------------
# Flow 3: pickoperations -> production star schema
# -----------------------------


def warn_if_dimension_key_will_multiply(conn, cfg: DBConfig) -> None:
    """
    Tableau Flow 3 joins fact rows to product only by product_id and to order only by order_id.
    If those keys are duplicated in the dimensions, the fact table will multiply rows.
    This warning does NOT change the logic; it only tells you why Flow 3 may be huge.
    """
    product_dups = fetch_one(conn, f"""
        SELECT COUNT(*) FROM (
            SELECT product_id
            FROM {qname(cfg.production_db, 'product')}
            GROUP BY product_id
            HAVING COUNT(*) > 1
        ) d
    """)[0]
    order_dups = fetch_one(conn, f"""
        SELECT COUNT(*) FROM (
            SELECT order_id
            FROM {qname(cfg.production_db, 'order')}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) d
    """)[0]

    if product_dups:
        log(f"WARNING: product has {product_dups:,} product_id values with more than one row. Flow 3 join can multiply fact rows.")
    if order_dups:
        log(f"WARNING: order has {order_dups:,} order_id values with more than one row. Flow 3 join can multiply fact rows.")


def flow3_staging_to_production(conn, cfg: DBConfig, db_batch_size: int, reset_output: bool) -> None:
    """
    FAST Flow 3: obeta_staging.pickoperations -> obeta_production physical star schema.

    Best approach for this project:
    - Python only orchestrates.
    - MySQL performs the heavy work internally.
    - No pandas processing for 33M+ rows.
    - No LIMIT/OFFSET batching.
    - No foreign keys before loading.
    - Production tables are physically written.
    - Indexes are created after the big load.
    - Lookup tables prevent row multiplication in the fact table.
    """
    log("START Flow 3 FAST SQL: pickoperations -> production physical tables")
    ensure_databases(conn, cfg)
    execute(conn, f"USE `{cfg.staging_db}`")

    execute(conn, "SET SESSION foreign_key_checks = 0")
    execute(conn, "SET SESSION unique_checks = 0")

    if reset_output:
        log("Dropping existing production tables")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'pick')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product')}")
        execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order')}")

    log("Creating production product dimension")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'product')} (
            product_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            product_id VARCHAR(50),
            product_description VARCHAR(255),
            product_group VARCHAR(100),
            warehouse_section VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    log("Creating production order dimension")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'order')} (
            order_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            order_id VARCHAR(50),
            origin VARCHAR(50),
            position_in_order VARCHAR(20)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    log("Creating production pick fact table")
    execute(conn, f"""
        CREATE TABLE IF NOT EXISTS {qname(cfg.production_db, 'pick')} (
            pick_id VARCHAR(50),
            product_key INT,
            order_key INT,
            pick_volume INT,
            quantity_unit VARCHAR(10),
            year INT,
            month INT,
            quarter INT,
            order_duration DECIMAL(12,6)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    if reset_output:
        log("Truncating production tables")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'pick')}")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'product')}")
        execute(conn, f"TRUNCATE TABLE {qname(cfg.production_db, 'order')}")

    log("Loading product dimension directly from staging")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'product')} (
            product_id,
            product_description,
            product_group,
            warehouse_section
        )
        SELECT DISTINCT
            product_id,
            product_description,
            product_group,
            warehouse_section
        FROM {qname(cfg.staging_db, 'pickoperations')}
    """)
    conn.commit()

    log("Loading order dimension directly from staging")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'order')} (
            order_id,
            origin,
            position_in_order
        )
        SELECT DISTINCT
            order_id,
            origin,
            position_in_order
        FROM {qname(cfg.staging_db, 'pickoperations')}
    """)
    conn.commit()

    log("Creating indexes for dimension lookup")
    add_index_if_missing(conn, cfg.production_db, "product", "idx_product_product_id", "product_id")
    add_index_if_missing(conn, cfg.production_db, "order", "idx_order_order_id", "order_id")
    conn.commit()

    log("Creating physical product lookup table")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
    execute(conn, f"""
        CREATE TABLE {qname(cfg.production_db, 'product_lookup')} (
            product_id VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL,
            product_key INT NOT NULL,
            PRIMARY KEY(product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'product_lookup')} (
            product_id,
            product_key
        )
        SELECT
            product_id COLLATE utf8mb4_unicode_ci AS product_id,
            MIN(product_key) AS product_key
        FROM {qname(cfg.production_db, 'product')}
        GROUP BY product_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Creating physical order lookup table")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
    execute(conn, f"""
        CREATE TABLE {qname(cfg.production_db, 'order_lookup')} (
            order_id VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL,
            order_key INT NOT NULL,
            PRIMARY KEY(order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'order_lookup')} (
            order_id,
            order_key
        )
        SELECT
            order_id COLLATE utf8mb4_unicode_ci AS order_id,
            MIN(order_key) AS order_key
        FROM {qname(cfg.production_db, 'order')}
        GROUP BY order_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Loading pick fact table with one internal SQL operation")
    execute(conn, f"""
        INSERT INTO {qname(cfg.production_db, 'pick')} (
            pick_id,
            product_key,
            order_key,
            pick_volume,
            quantity_unit,
            year,
            month,
            quarter,
            order_duration
        )
        SELECT
            s.pick_id,
            p.product_key,
            o.order_key,
            s.pick_volume,
            s.quantity_unit,
            s.year,
            s.month,
            s.quarter,
            s.order_duration
        FROM {qname(cfg.staging_db, 'pickoperations')} s
        JOIN {qname(cfg.production_db, 'product_lookup')} p
            ON s.product_id COLLATE utf8mb4_unicode_ci = p.product_id COLLATE utf8mb4_unicode_ci
        JOIN {qname(cfg.production_db, 'order_lookup')} o
            ON s.order_id COLLATE utf8mb4_unicode_ci = o.order_id COLLATE utf8mb4_unicode_ci
    """)
    conn.commit()

    log("Creating fact table indexes after load")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_product_key", "product_key")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_order_key", "order_key")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_year_month", "year, month")
    add_index_if_missing(conn, cfg.production_db, "pick", "idx_pick_quarter", "quarter")
    conn.commit()

    log("Dropping helper lookup tables after fact load")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'product_lookup')}")
    execute(conn, f"DROP TABLE IF EXISTS {qname(cfg.production_db, 'order_lookup')}")
    conn.commit()

    log("Adding physical foreign key constraints at the end")

    execute(conn, "SET SESSION unique_checks = 1")
    execute(conn, "SET SESSION foreign_key_checks = 1")
    conn.commit()

    fk_product_exists = fetch_one(conn, """
        SELECT COUNT(*)
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE table_schema = %s
          AND table_name = 'pick'
          AND constraint_name = 'fk_pick_product'
          AND referenced_table_name = 'product'
    """, (cfg.production_db,))[0]

    if not fk_product_exists:
        execute(conn, f"""
            ALTER TABLE {qname(cfg.production_db, 'pick')}
            ADD CONSTRAINT fk_pick_product
            FOREIGN KEY (product_key)
            REFERENCES {qname(cfg.production_db, 'product')}(product_key)
        """)
        conn.commit()
        log("Foreign key created: pick.product_key -> product.product_key")
    else:
        log("Foreign key already exists: fk_pick_product")

    fk_order_exists = fetch_one(conn, """
        SELECT COUNT(*)
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE table_schema = %s
          AND table_name = 'pick'
          AND constraint_name = 'fk_pick_order'
          AND referenced_table_name = 'order'
    """, (cfg.production_db,))[0]

    if not fk_order_exists:
        execute(conn, f"""
            ALTER TABLE {qname(cfg.production_db, 'pick')}
            ADD CONSTRAINT fk_pick_order
            FOREIGN KEY (order_key)
            REFERENCES {qname(cfg.production_db, 'order')}(order_key)
        """)
        conn.commit()
        log("Foreign key created: pick.order_key -> order.order_key")
    else:
        log("Foreign key already exists: fk_pick_order")

    log("END Flow 3 FAST SQL")


# -----------------------------
# Validation / diagnostics
# -----------------------------


def print_counts(conn, cfg: DBConfig) -> None:
    tables = [
        (cfg.staging_db, "warehouse_sections"),
        (cfg.staging_db, "product_data"),
        (cfg.staging_db, "pick_data"),
        (cfg.staging_db, "pickoperations"),
        (cfg.production_db, "product"),
        (cfg.production_db, "order"),
        (cfg.production_db, "pick"),
    ]
    log("Row counts")
    for db, table in tables:
        exists = fetch_one(conn, """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (db, table))[0]
        if not exists:
            print(f"  {db}.{table}: table does not exist")
            continue
        count = fetch_one(conn, f"SELECT COUNT(*) FROM {qname(db, table)}")[0]
        print(f"  {db}.{table}: {count:,}")


# -----------------------------
# CLI
# -----------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OBETA Tableau Prep flows rewritten in Python with batched DB writes")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--csv-chunk-size", type=int, default=50_000, help="Rows per pandas CSV chunk")
        p.add_argument("--db-batch-size", type=int, default=100_000, help="Rows/source IDs per DB batch commit")
        p.add_argument("--reset-output", action="store_true", help="Truncate/drop target tables before writing")
        p.add_argument("--product-encoding", default="cp1252", help="Encoding for 002 product_data.csv")

    p1 = sub.add_parser("flow1", help="Run Flow 1 only: raw CSV -> staging")
    add_common(p1)
    p1.add_argument("--warehouse-csv", required=True, type=Path)
    p1.add_argument("--product-csv", required=True, type=Path)
    p1.add_argument("--pick-csv", required=True, type=Path)

    p2 = sub.add_parser("flow2", help="Run Flow 2 only: staging -> pickoperations")
    add_common(p2)

    p3 = sub.add_parser("flow3", help="Run Flow 3 only: pickoperations -> production")
    add_common(p3)

    pall = sub.add_parser("run-all", help="Run Flow 1, Flow 2 and Flow 3")
    add_common(pall)
    pall.add_argument("--warehouse-csv", required=True, type=Path)
    pall.add_argument("--product-csv", required=True, type=Path)
    pall.add_argument("--pick-csv", required=True, type=Path)

    pc = sub.add_parser("counts", help="Print row counts for staging and production tables")
    add_common(pc)

    return parser


def validate_paths(args: argparse.Namespace) -> None:
    for attr in ["warehouse_csv", "product_csv", "pick_csv"]:
        path = getattr(args, attr, None)
        if path is not None and not path.exists():
            raise FileNotFoundError(f"File not found: {path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_paths(args)

    cfg = DBConfig.from_env()
    started = time.time()

    log(f"Command selected: {args.command}")
    log(f"Database: {cfg.user}@{cfg.host}:{cfg.port}; staging={cfg.staging_db}; production={cfg.production_db}")

    conn = connect(cfg)
    try:
        if args.command == "flow1":
            flow1_raw_to_staging(
                conn, cfg, args.warehouse_csv, args.product_csv, args.pick_csv,
                args.csv_chunk_size, args.db_batch_size, args.reset_output, args.product_encoding
            )
        elif args.command == "flow2":
            flow2_transform_staging(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "flow3":
            flow3_staging_to_production(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "run-all":
            flow1_raw_to_staging(
                conn, cfg, args.warehouse_csv, args.product_csv, args.pick_csv,
                args.csv_chunk_size, args.db_batch_size, args.reset_output, args.product_encoding
            )
            flow2_transform_staging(conn, cfg, args.db_batch_size, args.reset_output)
            flow3_staging_to_production(conn, cfg, args.db_batch_size, args.reset_output)
        elif args.command == "counts":
            print_counts(conn, cfg)
        else:
            parser.error(f"Unknown command: {args.command}")

        print_counts(conn, cfg)
        elapsed = time.time() - started
        log(f"DONE in {elapsed:,.1f} seconds")
        return 0
    except Exception as exc:
        conn.rollback()
        log(f"ERROR: {type(exc).__name__}: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
