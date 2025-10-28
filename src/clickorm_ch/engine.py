# src/clickorm_ch/engine.py

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Iterable, Union, AsyncIterator
import re, uuid
import aiohttp
import clickhouse_connect

from .query import Query
from .model import Base, Column
from .types import (CHType, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
                    Float32, Float64, Decimal, String, FixedString, UUID, Bool,
                    Date, Date32, DateTime, DateTime64, Nullable, Array, LowCardinality)
from .dialect import render_table_name, strip_any_quotes, quote_ident
from .compiler import Compiler

class ClickHouse:
    def __init__(self, host: str, port: int = 8123, user: str = "default",
                 password: str = "", database: str = "default", secure: bool = False, **kwargs: Any):
        self._client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database, secure=secure, **kwargs
        )
        self._model_cache: Dict[str, type] = {}
        self.debug: bool = False

    def session(self) -> "Session":
        return Session(self)

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        res = self._client.query(sql, parameters=params or {})
        cols = list(res.column_names) if hasattr(res, "column_names") else []
        out: List[Dict[str, Any]] = []
        for r in getattr(res, "result_rows", []):
            out.append({cols[i]: r[i] for i in range(len(cols))} if cols else r)
        return out

    def scalar(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        res = self._client.query(sql, parameters=params or {})
        row0 = res.result_rows[0]
        return row0[0] if isinstance(row0, (list, tuple)) else row0

    def describe_table(self, table: str) -> List[Dict[str, Any]]:
        return self.execute(f"DESCRIBE TABLE {render_table_name(table)}")

    def generate_model(self, table: str, class_name: Optional[str] = None, cache: bool = True) -> type:
        key = table.strip()
        if cache and key in self._model_cache:
            return self._model_cache[key]
        desc = self.describe_table(table)
        attrs: Dict[str, Any] = {"__table__": table}
        for col in desc:
            name = col.get("name") or col.get("column", "unnamed")
            ch_type_str = col.get("type") or ""
            attrs[name] = Column(_parse_ch_type(ch_type_str))
        cls_name = class_name or _derive_class_name_from_table(table)
        model_cls = type(cls_name, (Base,), attrs)
        if cache:
            self._model_cache[key] = model_cls
        return model_cls

    def raw(self, sql: str, params: Optional[Dict[str, Any]] = None):
        return self.execute(sql, params)

    def table_exists(self, name: str) -> bool:
        try:
            return bool(self.scalar(f"EXISTS TABLE {render_table_name(name)}"))
        except Exception:
            return False

    def _model_columns(self, model_or_table: Union[type, str]) -> List[str]:
        if isinstance(model_or_table, str):
            desc = self.execute(f"DESCRIBE TABLE {render_table_name(model_or_table)}")
            return [r["name"] for r in desc]
        return [c.name for c in getattr(model_or_table, "__columns__", {}).values()]

    async def stream_csv(self, target: Union[type, str], byte_iter: AsyncIterator[bytes], *,
                         with_names: bool = True, allow_errors_ratio: float = 0.0,
                         best_effort_datetime: bool = True, wait_for_async_insert: bool = False,
                         query_id: Optional[str] = None) -> Dict[str, Any]:
        host = getattr(self._client, "host", "localhost")
        port = getattr(self._client, "port", 8123)
        db   = getattr(self._client, "database", "default")
        user = getattr(self._client, "username", None)
        pwd  = getattr(self._client, "password", None)

        base = f"http://{host}:{port}/?database={db}"
        qid = query_id or f"etl_{uuid.uuid4().hex}"
        params = [f"query_id={qid}"]
        if best_effort_datetime:
            params.append("date_time_input_format=best_effort")
        if allow_errors_ratio and allow_errors_ratio > 0:
            params.append(f"input_format_allow_errors_ratio={allow_errors_ratio}")
        if wait_for_async_insert:
            params.append("wait_for_async_insert=1")
            params.append("input_format_parallel_parsing=1")
        url = base + "&" + "&".join(params)

        fmt = "CSVWithNames" if with_names else "CSV"
        tbl = render_table_name(getattr(target, "__table__", target) if not isinstance(target, str) else target)
        auth = aiohttp.BasicAuth(user, pwd) if user else None
        timeout = aiohttp.ClientTimeout(total=None, sock_read=None, sock_connect=None)

        async def body():
            yield f"INSERT INTO {tbl} FORMAT {fmt}\n".encode()
            async for chunk in byte_iter:
                if chunk:
                    yield chunk

        async with aiohttp.ClientSession(timeout=timeout, auth=auth) as sess:
            async with sess.post(url, data=body()) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise ValueError(text)

            rows_after = None
            try:
                count_sql = f"SELECT count() FROM {tbl}"
                async with sess.post(base, data=count_sql) as c_resp:
                    if c_resp.status == 200:
                        raw = (await c_resp.text()).strip()
                        rows_after = int(raw) if raw.isdigit() else None
            except Exception:
                pass

        return {"rows_after": rows_after, "query_id": qid}

class Session:
    def __init__(self, engine: ClickHouse):
        self.engine = engine

    def query(self, model) -> Query:
        return Query(self.engine, model)

    def insert_rows(self, target: Union[type, str], rows: List[Iterable[Any]], *,
                    columns: Optional[List[str]] = None, on_cluster: Optional[str] = None) -> Dict[str, Any]:
        tbl = render_table_name(getattr(target, "__table__", target) if not isinstance(target, str) else target)
        cols = columns or self.engine._model_columns(target)
        cols_quoted = ", ".join(quote_ident(c) for c in cols)
        values_sql = ", ".join("(" + ", ".join(f"%({f'v_{i}_{j}'})s" for j, _ in enumerate(cols)) + ")"
                               for i, _ in enumerate(rows))
        params: Dict[str, Any] = {}
        for i, row in enumerate(rows):
            for j, v in enumerate(row):
                params[f"v_{i}_{j}"] = v
        sql = f"INSERT INTO {tbl} ({cols_quoted}) VALUES {values_sql}"
        if self.engine.debug:
            print("[SQL]", sql, params)
        self.engine._client.command(sql, parameters=params)
        return {"inserted": None, "sql": sql, "query_id": None}

    def insert_dicts(self, target: Union[type, str], items: List[Dict[str, Any]], *,
                     columns: Optional[List[str]] = None, on_cluster: Optional[str] = None) -> Dict[str, Any]:
        cols = columns or sorted({k for d in items for k in d.keys()})
        rows = [[d.get(c) for c in cols] for d in items]
        return self.insert_rows(target, rows, columns=cols, on_cluster=on_cluster)

    def insert_from_select(self, target: Union[type, str], select: Union[str, "Query"], *,
                           columns: Optional[List[str]] = None, safe: bool = False) -> Dict[str, Any]:
        tbl = render_table_name(getattr(target, "__table__", target) if not isinstance(target, str) else target)
        if hasattr(select, "all"):  # наш Query
            comp = Compiler()
            sql_sel, params = comp.select(select.model, select._where, select._order_by,
                                          select._limit, select._offset)
        else:
            sql_sel, params = str(select), {}
        cols = columns or self.engine._model_columns(target)
        cols_quoted = ", ".join(quote_ident(c) for c in cols)
        sql = f"INSERT INTO {tbl} ({cols_quoted}) {sql_sel}"
        if self.engine.debug:
            print("[SQL]", sql, params)
        self.engine._client.command(sql, parameters=params)
        return {"inserted": None, "sql": sql, "query_id": None}

    def insert_builder(self, target):
        from .insert_builder import InsertBuilder
        return InsertBuilder(self, target)
    
_SIMPLE_MAP: Dict[str, CHType] = {
    "int8": Int8(), "int16": Int16(), "int32": Int32(), "int64": Int64(),
    "uint8": UInt8(), "uint16": UInt16(), "uint32": UInt32(), "uint64": UInt64(),
    "float32": Float32(), "float64": Float64(),
    "string": String(),
    "uuid": UUID(),
    "bool": Bool(),
    "boolean": Bool(),
    "date": Date(),
    "date32": Date32(),
    "datetime": DateTime(),
}

_RE_DECIMAL = re.compile(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.I)
_RE_FIXED   = re.compile(r"^fixedstring\s*\(\s*(\d+)\s*\)\s*$", re.I)
_RE_DT64    = re.compile(r"^datetime64\s*\(\s*(\d+)\s*\)\s*$", re.I)
_RE_WRAP1   = re.compile(r"^(nullable|array|lowcardinality)\s*\((.*)\)$", re.I)


def _parse_ch_type(t: str) -> CHType:
    s = t.strip()
    m = _RE_WRAP1.match(s)
    if m:
        wrap = m.group(1).lower()
        inner_raw = m.group(2).strip()
        inner = _parse_ch_type(inner_raw)
        if wrap == "nullable":
            return Nullable(inner)
        if wrap == "array":
            return Array(inner)
        if wrap == "lowcardinality":
            return LowCardinality(inner)

    m = _RE_DECIMAL.match(s)
    if m:
        return Decimal(int(m.group(1)), int(m.group(2)))

    m = _RE_FIXED.match(s)
    if m:
        return FixedString(int(m.group(1)))

    m = _RE_DT64.match(s)
    if m:
        return DateTime64(int(m.group(1)))

    key = s.lower()
    key = key.split()[0]
    if key in _SIMPLE_MAP:
        return _SIMPLE_MAP[key]

    return String()

def _derive_class_name_from_table(table: str) -> str:
    """
    "db.tbl_name" -> "TblName"
    "`My Table`"  -> "MyTable"
    """
    s = strip_any_quotes(table)
    if "." in s:
        s = s.split(".", 1)[1]
    parts = re.split(r"[^0-9A-Za-z]+", s)
    cand = "".join(p.capitalize() for p in parts if p)
    return cand or "AutoTable"