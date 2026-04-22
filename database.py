"""
database.py — poenostavljena verzija brez SQLite.
Podatki živijo samo v Streamlit session_state (in-memory).
Primerno za Streamlit Cloud kjer se nalaža Excel ročno.
"""

import pandas as pd
from datetime import datetime

EXPECTED_TABLES = ['transactions', 'sp_transactions', 'budget_plan', 'income_history', 'accounts']

# In-memory "baza" — živi samo med sejo
_store = {}
_last_import = None


def db_exists():
    """Vrne True če so podatki že naloženi v memorijo."""
    return bool(_store)


def save_to_db(data_dict):
    """Shrani vse dataframe-e v memory. Vrne (True, '') ali (False, error_msg)."""
    global _store, _last_import
    try:
        _store = {}
        for table_name, df in data_dict.items():
            if df is not None and not df.empty:
                _store[table_name] = df.copy()
        _last_import = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        return True, ''
    except Exception as e:
        return False, str(e)


def load_from_db():
    """Vrne shranjene dataframe-e ali None."""
    if not _store:
        return None
    return {k: v.copy() for k, v in _store.items()}


def get_db_info():
    """Vrne info o vsaki tabeli: {table_name: {'rows': int, 'cols': int, 'columns': list}}."""
    info = {}
    for table_name, df in _store.items():
        info[table_name] = {
            'rows': len(df),
            'cols': len(df.columns),
            'columns': df.columns.tolist()
        }
    return info


def get_last_import():
    """Vrne čas zadnjega uvoza ali None."""
    return _last_import


def get_db_size_kb():
    """Vrne ocenjeno velikost podatkov v KB (in-memory)."""
    if not _store:
        return 0
    total_bytes = sum(df.memory_usage(deep=True).sum() for df in _store.values())
    return round(total_bytes / 1024, 1)


def clear_db():
    """Počisti vse podatke iz memorije."""
    global _store, _last_import
    _store = {}
    _last_import = None
