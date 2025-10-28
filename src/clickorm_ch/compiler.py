# src/clickorm_ch/compiler.py

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .dialect import quote_ident, render_table_name
from .expressions import Expr, ColumnExpr

class Compiler:
    def __init__(self):
        self._i = 0

    def add_param(self, v: Any) -> str:
        p = f"p{self._i}"
        self._i += 1
        return p

    def select(
        self,
        model,
        where: Optional[Expr],
        order_by: Optional[List[Tuple[ColumnExpr, str]]],
        limit: Optional[int],
        offset: Optional[int],
    ) -> Tuple[str, Dict[str, Any]]:
        cols = ", ".join(quote_ident(c.name) for c in model.__columns__.values()) or "*"
        tbl = render_table_name(getattr(model, "__table__", model.__name__.lower()))
        sql = f"SELECT {cols} FROM {tbl}"
        params: Dict[str, Any] = {}

        if where is not None:
            ws, wp = where.to_sql(self)
            sql += f" WHERE {ws}"
            params.update(wp)
        if order_by:
            parts = [f"{quote_ident(col.name)} {direction}" for col, direction in order_by]
            sql += " ORDER BY " + ", ".join(parts)
        if limit is not None:
            pname = self.add_param(limit)
            sql += f" LIMIT %({pname})s"
            params[pname] = limit
        if offset is not None:
            pname = self.add_param(offset)
            sql += f" OFFSET %({pname})s"
            params[pname] = offset

        return sql, params
