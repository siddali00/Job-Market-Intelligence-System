from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

from storage.db import engine


_IDENT_RE = re.compile(r"[^a-z0-9_]+")


def _sanitize_identifier(value: str, *, prefix: str, max_len: int = 63) -> str:
    base = (value or "").strip().lower()
    base = base.replace("-", "_").replace(" ", "_")
    base = _IDENT_RE.sub("_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = prefix
    if base[0].isdigit():
        base = f"{prefix}_{base}"
    return base[:max_len]


def _table_name(dataset_key: str, source_file: str) -> str:
    file_stem = Path(source_file).stem
    ds = _sanitize_identifier(dataset_key, prefix="dataset")
    stem = _sanitize_identifier(file_stem, prefix="file")
    base = f"kaggle_raw_{ds}_{stem}"
    if len(base) <= 63:
        return base
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"{base[:54]}_{digest}"


def _text_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


@dataclass
class RawTableSpec:
    table_name: str
    column_map: dict[str, str]


class KaggleRawStore:
    """
    Writes Kaggle raw rows into relational per-file tables.
    """

    def __init__(self, run_id: str | None) -> None:
        self.run_id = run_id
        self._cache: dict[tuple[str, str], RawTableSpec] = {}

    def ensure_table(self, dataset_key: str, source_file: str, columns: list[str]) -> RawTableSpec:
        cache_key = (dataset_key, source_file)
        if cache_key in self._cache:
            return self._cache[cache_key]

        table = _table_name(dataset_key, source_file)
        reserved = {"id", "run_id", "ingested_at", "dataset_key", "source_file", "row_number"}
        col_map: dict[str, str] = {}
        for idx, col in enumerate(columns):
            raw_name = str(col).strip() or f"col_{idx + 1}"
            sanitized = _sanitize_identifier(raw_name, prefix="col")
            # Avoid collisions with metadata columns (especially "id")
            if sanitized in reserved:
                sanitized = f"src_{sanitized}"
            if sanitized in col_map.values():
                sanitized = f"{sanitized}_{idx + 1}"
            col_map[raw_name] = sanitized

        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS "{table}" (
                    id BIGSERIAL PRIMARY KEY,
                    run_id VARCHAR(36) NULL,
                    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    dataset_key VARCHAR(100) NOT NULL,
                    source_file VARCHAR(255) NOT NULL,
                    row_number BIGINT NOT NULL
                )
            """))

            existing = {
                row[0]
                for row in conn.execute(text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table
                """), {"table": table})
            }

            for _, db_col in col_map.items():
                if db_col not in existing:
                    conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{db_col}" TEXT'))

        spec = RawTableSpec(table_name=table, column_map=col_map)
        self._cache[cache_key] = spec
        return spec

    def insert_rows(
        self,
        dataset_key: str,
        source_file: str,
        rows: list[dict[str, Any]],
        *,
        row_start: int = 1,
    ) -> int:
        if not rows:
            return 0

        spec = self.ensure_table(dataset_key, source_file, list(rows[0].keys()))
        db_cols = [spec.column_map[k] for k in rows[0].keys()]
        insert_cols = ["run_id", "dataset_key", "source_file", "row_number", *db_cols]
        quoted_cols = ", ".join(f'"{c}"' for c in insert_cols)
        placeholders = ", ".join(f":{c}" for c in insert_cols)

        stmt = text(f'INSERT INTO "{spec.table_name}" ({quoted_cols}) VALUES ({placeholders})')
        mappings: list[dict[str, Any]] = []
        for i, row in enumerate(rows, start=row_start):
            payload: dict[str, Any] = {
                "run_id": self.run_id,
                "dataset_key": dataset_key,
                "source_file": source_file,
                "row_number": i,
            }
            for src_col, db_col in spec.column_map.items():
                payload[db_col] = _text_value(row.get(src_col))
            mappings.append(payload)

        with engine.begin() as conn:
            conn.execute(stmt, mappings)
        return len(mappings)
