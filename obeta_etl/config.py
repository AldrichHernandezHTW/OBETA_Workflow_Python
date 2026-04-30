from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


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