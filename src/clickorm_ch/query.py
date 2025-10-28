# src/clickorm_ch/query.py

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .expressions import Expr, LogicalExpr, ColumnExpr
from .compiler import Compiler
from typing import Union
from .expressions import Expr, LogicalExpr, ColumnExpr, ValueExpr

class Query:
    def __init__(self, engine, model):
        self.engine = engine
        self.model = model
        self._where: Optional[Expr] = None
        self._order_by: Optional[List[Tuple[ColumnExpr, str]]] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def filter(self, expr: Union[Expr, str]) -> "Query":
        if isinstance(expr, str):
            class _Raw(Expr):
                def __init__(self, sql: str): self.sql = sql
                def to_sql(self, compiler): return self.sql, {}
            expr = _Raw(expr)

        if self._where is None:
            self._where = expr
        else:
            self._where = LogicalExpr(self._where, "AND", expr)
        return self

    def order_by(self, *cols: Tuple[ColumnExpr, str]) -> "Query":
        self._order_by = list(cols)
        return self

    def limit(self, n: int) -> "Query":
        self._limit = int(n)
        return self

    def offset(self, n: int) -> "Query":
        self._offset = int(n)
        return self

    def all(self) -> List[Dict[str, Any]]:
        comp = Compiler()
        sql, params = comp.select(self.model, self._where, self._order_by, self._limit, self._offset)
        return self.engine.execute(sql, params)

    def first(self) -> Optional[Dict[str, Any]]:
        if self._limit is None or self._limit > 1:
            self._limit = 1
        rows = self.all()
        return rows[0] if rows else None

    def count(self) -> int:
        comp = Compiler()
        sql, params = comp.select(self.model, self._where, self._order_by, None, None)
        wrapped = f'SELECT count() FROM ({sql}) AS "sub"'
        return self.engine.scalar(wrapped, params)