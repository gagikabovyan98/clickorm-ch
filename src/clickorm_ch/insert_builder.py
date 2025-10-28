#  /src/clickorm_ch/insert_builder.py

from __future__ import annotations
from typing import Any, Dict, List, Optional, Iterable, Union
from .dialect import render_table_name, quote_ident
from .model import Base

class T:
    def __init__(self, alias: str, name: str):
        self.alias = alias
        self.name = name

class InsertBuilder:
    def __init__(self, session, target: Union[type[Base], str]):
        self.session = session
        self.target = target
        self._sources: List[T] = []
        self._mapping: Dict[str, str] = {}
        self._joins: List[str] = []
        self._where: List[str] = []
        self._group_by: List[str] = []
        self._order_by: List[str] = []
        self._validated: bool = False

    def sources(self, *tables: T) -> "InsertBuilder":
        self._sources.extend(tables); return self

    def map(self, mapping: Dict[str, str]) -> "InsertBuilder":
        self._mapping.update(mapping); return self

    def join_on(self, joins: Iterable[str]) -> "InsertBuilder":
        self._joins.extend(joins); return self

    def where(self, filters: Iterable[str]) -> "InsertBuilder":
        self._where.extend(filters); return self

    def group_by(self, cols: Iterable[str]) -> "InsertBuilder":
        self._group_by.extend(cols); return self

    def order_by(self, cols: Iterable[str]) -> "InsertBuilder":
        self._order_by.extend(cols); return self

    def validate(self, *, allowed_tables: Optional[set[str]] = None,
                 strict_expected_coverage: bool = False,
                 assert_safe: bool = True) -> "InsertBuilder":
        self._validated = True
        return self

    def compile(self) -> Dict[str, str]:
        assert self._sources, "No sources() specified"
        from_clause = f'{render_table_name(self._sources[0].name)} AS {self._sources[0].alias}'
        for i in range(1, len(self._sources)):
            on = (self._joins[i-1] if i-1 < len(self._joins) else "")
            from_clause += f' JOIN {render_table_name(self._sources[i].name)} AS {self._sources[i].alias}'
            if on:
                from_clause += f' ON {on}'

        assert self._mapping, "No map() specified"
        select_items = [f'{expr} AS {quote_ident(col)}' for col, expr in self._mapping.items()]
        select_clause = ", ".join(select_items)

        where_clause = f' WHERE {" AND ".join(self._where)}' if self._where else ""
        group_clause = f' GROUP BY {", ".join(self._group_by)}' if self._group_by else ""
        order_clause = f' ORDER BY {", ".join(self._order_by)}' if self._order_by else ""
        select_sql = f"SELECT {select_clause} FROM {from_clause}{where_clause}{group_clause}{order_clause}"

        cols = list(self._mapping.keys())
        cols_quoted = ", ".join(quote_ident(c) for c in cols)
        tbl = render_table_name(getattr(self.target, "__table__", self.target) if not isinstance(self.target, str) else self.target)
        insert_sql = f"INSERT INTO {tbl} ({cols_quoted}) {select_sql}"
        return {"insert_sql": insert_sql, "select_sql": select_sql}

    def execute(self) -> Dict[str, Any]:
        sqls = self.compile()
        return self.session.insert_from_select(self.target, sqls["select_sql"], columns=list(self._mapping.keys()))

def T_(alias: str, name: str) -> T:
    return T(alias, name)
