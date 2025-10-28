# src/clickorm_ch/expressions.py

from __future__ import annotations
from typing import Any, Dict, Iterable, Tuple
from .dialect import quote_ident

class CompilerProtocol:
    def add_param(self, v: Any) -> str: ...

class Expr:
    def to_sql(self, compiler: CompilerProtocol) -> Tuple[str, Dict[str, Any]]:
        raise NotImplementedError

class ColumnExpr(Expr):
    def __init__(self, model_cls, name: str):
        self.model_cls = model_cls
        self.name = name

    def __eq__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "=", ValueExpr(other))

    def __ne__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "!=", ValueExpr(other))

    def __gt__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, ">", ValueExpr(other))

    def __lt__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "<", ValueExpr(other))

    def __ge__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, ">=", ValueExpr(other))

    def __le__(self, other: Any) -> "BinaryExpr":
        return BinaryExpr(self, "<=", ValueExpr(other))

    def in_(self, items: Iterable[Any]) -> "BinaryExpr":
        return BinaryExpr(self, "IN", ValueExpr(list(items)))

    def like(self, pattern: str) -> "BinaryExpr":
        return BinaryExpr(self, "LIKE", ValueExpr(pattern))

    def to_sql(self, compiler: CompilerProtocol):
        return quote_ident(self.name), {}

class ValueExpr(Expr):
    def __init__(self, value: Any):
        self.value = value
    def to_sql(self, compiler: CompilerProtocol):
        pname = compiler.add_param(self.value)
        return f"%({pname})s", {pname: self.value}

class BinaryExpr(Expr):
    def __init__(self, left: Expr, op: str, right: Expr):
        self.left, self.op, self.right = left, op, right
    def __and__(self, other: "Expr") -> "LogicalExpr":
        return LogicalExpr(self, "AND", other)
    def __or__(self, other: "Expr") -> "LogicalExpr":
        return LogicalExpr(self, "OR", other)
    def to_sql(self, compiler: CompilerProtocol):
        ls, lp = self.left.to_sql(compiler)
        rs, rp = self.right.to_sql(compiler)
        return f"({ls} {self.op} {rs})", {**lp, **rp}

class LogicalExpr(BinaryExpr):
    pass
