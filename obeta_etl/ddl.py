from __future__ import annotations

from obeta_etl.config import DBConfig
from obeta_etl.db import execute, qname, truncate_or_drop
from obeta_etl.logger import log


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