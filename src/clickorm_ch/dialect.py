# src/clickorm_ch/dialect.py

from __future__ import annotations

def strip_any_quotes(name: str) -> str:
    if not name:
        return name
    s = str(name).strip()
    if (s.startswith('"') and s.endswith('"')) or \
       (s.startswith('`') and s.endswith('`')) or \
       (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def quote_ident(name: str) -> str:
    raw = strip_any_quotes(str(name))
    return '"' + raw.replace('"', '""') + '"'

def render_table_name(name: str) -> str:
    s = str(name).strip()
    if "." in s:
        db, tbl = s.split(".", 1)
        return f'{quote_ident(db)}.{quote_ident(tbl)}'
    return quote_ident(s)
