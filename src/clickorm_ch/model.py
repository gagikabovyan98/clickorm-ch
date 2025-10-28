# src/clickorm_ch/model.py

from __future__ import annotations
from typing import Dict, Optional, List
from .expressions import ColumnExpr

class Column:
    def __init__(self, ch_type, primary_key: bool = False, name: Optional[str] = None):
        self.ch_type = ch_type
        self.primary_key = primary_key
        self.name = name

class _Metadata:
    def __init__(self):
        self.models: List[type["Base"]] = []

    def create_all(self, bind):
        from .ddl import create_all as _create_all
        _create_all(bind)

    def drop_all(self, bind):
        from .ddl import drop_table
        for m in list(self.models):
            tbl = getattr(m, "__table__", m.__name__.lower())
            drop_table(bind, tbl, if_exists=True)

class ModelMeta(type):
    def __new__(mcls, name, bases, attrs):
        if name == "Base":
            return super().__new__(mcls, name, bases, attrs)

        columns = {}
        for k, v in list(attrs.items()):
            if isinstance(v, Column):
                v.name = v.name or k
                columns[k] = v

        cls = super().__new__(mcls, name, bases, attrs)
        cls.__columns__: Dict[str, Column] = columns
        cls.__table__: str = attrs.get("__table__", name.lower())

        for py_name, col in columns.items():
            setattr(cls, py_name, ColumnExpr(cls, col.name))

        Base.metadata.models.append(cls)
        return cls

class Base(metaclass=ModelMeta):
    __table__: str
    __columns__: Dict[str, Column]
    metadata = _Metadata()

    @classmethod
    def create(cls, bind, if_not_exists: bool = True):
        from .ddl import create_table_from_model
        create_table_from_model(bind, cls, if_not_exists=if_not_exists)

    @classmethod
    def drop(cls, bind, if_exists: bool = True):
        from .ddl import drop_table
        drop_table(bind, getattr(cls, "__table__", cls.__name__.lower()), if_exists=if_exists)