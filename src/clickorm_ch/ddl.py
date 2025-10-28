# src/clickorm_ch/ddl.py

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from .dialect import quote_ident, render_table_name
from .types import (
    CHType, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64, Decimal, String, FixedString, UUID, Bool,
    Date, Date32, DateTime, DateTime64, Nullable, Array, LowCardinality,
)
from .model import Base, Column


def _render_type(t: Union[CHType, str]) -> str:
    """Рендерит наши типы в ClickHouse-строку (учитывает Nullable/Array/LowCardinality)."""
    if isinstance(t, str):
        return t
    if isinstance(t, Int8): return "Int8"
    if isinstance(t, Int16): return "Int16"
    if isinstance(t, Int32): return "Int32"
    if isinstance(t, Int64): return "Int64"
    if isinstance(t, UInt8): return "UInt8"
    if isinstance(t, UInt16): return "UInt16"
    if isinstance(t, UInt32): return "UInt32"
    if isinstance(t, UInt64): return "UInt64"
    if isinstance(t, Float32): return "Float32"
    if isinstance(t, Float64): return "Float64"
    if isinstance(t, String): return "String"
    if isinstance(t, UUID): return "UUID"
    if isinstance(t, Bool): return "Bool"
    if isinstance(t, Date): return "Date"
    if isinstance(t, Date32): return "Date32"
    if isinstance(t, DateTime): return "DateTime"
    if isinstance(t, Decimal): return f"Decimal({t.precision},{t.scale})"
    if isinstance(t, FixedString): return f"FixedString({t.n})"
    if isinstance(t, DateTime64): return f"DateTime64({t.precision})"
    if isinstance(t, Nullable): return f"Nullable({_render_type(t.inner)})"
    if isinstance(t, Array): return f"Array({_render_type(t.inner)})"
    if isinstance(t, LowCardinality): return f"LowCardinality({_render_type(t.inner)})"
    return "String"

def _render_columns_from_dict(columns: Dict[str, Union[CHType, str]]) -> str:
    parts = []
    for name, t in columns.items():
        parts.append(f'{quote_ident(name)} {_render_type(t)}')
    return ",\n  ".join(parts)

def _render_columns_from_model(model: type[Base]) -> str:
    cols: Dict[str, Column] = getattr(model, "__columns__", {})
    parts = []
    for name, col in cols.items():
        parts.append(f'{quote_ident(col.name)} {_render_type(col.ch_type)}')
    return ",\n  ".join(parts)

def _render_settings(settings: Optional[Dict[str, Any]]) -> Optional[str]:
    if not settings:
        return None
    items = []
    for k, v in settings.items():
        if isinstance(v, bool):
            items.append(f"{k}={1 if v else 0}")
        elif isinstance(v, (int, float)):
            items.append(f"{k}={v}")
        else:
            items.append(f"{k}='{v}'")
    return "SETTINGS " + ", ".join(items)

def _render_indexes(indexes: Optional[List[Dict[str, str]]]) -> Optional[str]:
    if not indexes:
        return None
    parts = []
    for ix in indexes:
        name = quote_ident(ix["name"])
        expr = ix["expr"]
        t    = ix["type"]
        gran = ix.get("granularity")
        if gran:
            parts.append(f"INDEX {name} {expr} TYPE {t} GRANULARITY {gran}")
        else:
            parts.append(f"INDEX {name} {expr} TYPE {t}")
    return ",\n  ".join(parts)

# ---------- public API ----------

def create_table(
    db,
    name: str,
    columns: Dict[str, Union[CHType, str]],
    *,
    engine: str = "MergeTree",
    order_by: Optional[Iterable[str]] = None,
    partition_by: Optional[str] = None,
    primary_key: Optional[Iterable[str]] = None,
    ttl: Optional[str] = None,
    indexes: Optional[List[Dict[str, str]]] = None,
    settings: Optional[Dict[str, Any]] = None,
    if_not_exists: bool = True,
    comment: Optional[str] = None,
) -> None:
    """
    Создание таблицы по параметрам.
    - name: "db.table" или "table" (любой регистр/кавычки/армянские символы — всё безопасно)
    - columns: {"id": UInt64(), "Անուն": String(), "ts": "DateTime64(3)", ...}
    - order_by: ["id"] или ["id","ts"] — если None, попытаемся выбрать разумный default
    """
    tbl = render_table_name(name)

    cols_lower = {k.lower(): k for k in columns.keys()}
    if order_by is None:
        if "id" in cols_lower:
            order_by = [cols_lower["id"]]
        else:
            first_col = next(iter(columns.keys()))
            order_by = [first_col]

    cols_sql = _render_columns_from_dict(columns)

    parts = [f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{tbl}",
             "(\n  " + cols_sql]

    ix = _render_indexes(indexes)
    if ix:
        parts.append(",\n  " + ix)

    parts.append("\n)")
    parts.append(f"ENGINE = {engine}")

    if partition_by:
        parts.append(f"PARTITION BY {partition_by}")

    if primary_key:
        pk_q = ", ".join(quote_ident(c) for c in primary_key)
        parts.append(f"PRIMARY KEY ({pk_q})")

    if order_by:
        ob_q = ", ".join(quote_ident(c) for c in order_by)
        parts.append(f"ORDER BY ({ob_q})")

    if ttl:
        parts.append(f"TTL {ttl}")

    st_sql = _render_settings(settings)
    if st_sql:
        parts.append(st_sql)

    if comment:
        parts.append(f"COMMENT '{comment}'")

    sql = "\n".join(parts)
    db.execute(sql)

def create_table_from_model(
    db, model: type[Base], *, if_not_exists: bool = True
) -> None:
    table = getattr(model, "__table__", model.__name__.lower())
    engine = getattr(model, "__engine__", "MergeTree")
    order_by = getattr(model, "__order_by__", None)
    partition_by = getattr(model, "__partition_by__", None)
    primary_key = getattr(model, "__primary_key__", None)
    ttl = getattr(model, "__ttl__", None)
    settings = getattr(model, "__settings__", None)
    indexes = getattr(model, "__indexes__", None)
    comment = getattr(model, "__comment__", None)

    cols_sql = _render_columns_from_model(model)
    tbl = render_table_name(table)

    parts = [f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{tbl}",
             "(\n  " + cols_sql]

    ix = _render_indexes(indexes)
    if ix:
        parts.append(",\n  " + ix)

    parts.append("\n)")
    parts.append(f"ENGINE = {engine}")

    if partition_by:
        parts.append(f"PARTITION BY {partition_by}")

    if primary_key:
        pk_q = ", ".join(quote_ident(c) for c in primary_key)
        parts.append(f"PRIMARY KEY ({pk_q})")

    if order_by:
        ob_q = ", ".join(quote_ident(c) for c in order_by)
        parts.append(f"ORDER BY ({ob_q})")

    if ttl:
        parts.append(f"TTL {ttl}")

    st_sql = _render_settings(settings)
    if st_sql:
        parts.append(st_sql)

    if comment:
        parts.append(f"COMMENT '{comment}'")

    sql = "\n".join(parts)
    db.execute(sql)

def create_all(db, *, models: Optional[Iterable[type[Base]]] = None, if_not_exists: bool = True) -> None:
    items = list(models) if models is not None else list(Base.metadata.models)
    for m in items:
        create_table_from_model(db, m, if_not_exists=if_not_exists)

def drop_table(db, name: str, *, if_exists: bool = True) -> None:
    tbl = render_table_name(name)
    db.execute(f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{tbl}")
