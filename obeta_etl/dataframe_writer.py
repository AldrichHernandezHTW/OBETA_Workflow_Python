from __future__ import annotations

import pandas as pd

from obeta_etl.db import col_list, placeholders, qname


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
            batch = rows[start:start + batch_size]
            cur.executemany(sql, batch)
            inserted += cur.rowcount if cur.rowcount != -1 else len(batch)

    conn.commit()
    return inserted