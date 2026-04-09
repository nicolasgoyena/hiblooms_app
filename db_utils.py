# db_utils.py
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =========================
# Conexión
# =========================
def get_engine() -> Engine:
    """
    Crea y devuelve un Engine de SQLAlchemy a PostgreSQL usando st.secrets.
    Requiere sección [postgres] en .streamlit/secrets.toml con:
      host, port, dbname, user, password
    """
    if "postgres" not in st.secrets:
        raise RuntimeError("Configura [postgres] en .streamlit/secrets.toml")

    pg = st.secrets["postgres"]
    uri = (
        "postgresql+psycopg2://"
        f"{quote_plus(pg['user'])}:{quote_plus(pg['password'])}"
        f"@{pg['host']}:{pg.get('port', 5432)}/{pg['dbname']}"
    )
    return create_engine(uri, pool_pre_ping=True)


# =========================
# Metadatos
# =========================
def get_columns(engine: Engine, table: str, schema: str = "public") -> List[Dict[str, Any]]:
    """
    Devuelve lista de columnas con: name, type, nullable.
    Usa information_schema para ser agnóstico al esquema.
    """
    q = text("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """)
    with engine.connect() as con:
        rows = con.execute(q, {"schema": schema, "table": table}).mappings().all()

    cols: List[Dict[str, Any]] = []
    for r in rows:
        cols.append({
            "name": r["column_name"],
            "type": r["data_type"],
            "nullable": (r["is_nullable"] == "YES"),
        })
    return cols


def infer_pk(engine: Engine, table: str, schema: str = "public") -> Optional[str]:
    """
    Intenta inferir el nombre de la clave primaria usando pg_index.
    Devuelve None si no encuentra PK.
    """
    # Construye la referencia completa al estilo "public.mi_tabla"
    schema_table = f"{schema}.{table}"

    # Inyecta directamente el nombre, escapando comillas dobles si las hubiera
    query = f"""
        SELECT a.attname AS col
        FROM pg_index i
        JOIN pg_attribute a
          ON a.attrelid = i.indrelid
         AND a.attnum  = ANY(i.indkey)
        WHERE i.indrelid = '{schema_table}'::regclass
          AND i.indisprimary = true
        LIMIT 1;
    """

    with engine.connect() as con:
        r = con.execute(text(query)).first()

    return r[0] if r else None



# =========================
# Lectura con paginado y filtro
# =========================
def read_table(
    engine: Engine,
    table: str,
    schema: str,
    limit: int,
    offset: int,
    search: Optional[str],
    searchable_cols: List[str]
) -> Tuple[pd.DataFrame, int]:
    """
    Lee la tabla con paginado y filtro simple (ILIKE) sobre columnas indicadas.
    Devuelve (df, total_registros).
    """
    where = ""
    params: Dict[str, Any] = {}
    if search and searchable_cols:
        like_clauses = []
        for i, c in enumerate(searchable_cols):
            # Casting a TEXT para evitar problemas con tipos no textuales
            like_clauses.append(f'CAST("{c}" AS TEXT) ILIKE :q{i}')
            params[f"q{i}"] = f"%{search}%"
        where = "WHERE " + " OR ".join(like_clauses)

    count_sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" {where}'
    data_sql  = f'''
        SELECT * FROM "{schema}"."{table}"
        {where}
        ORDER BY 1 DESC
        LIMIT :lim OFFSET :off
    '''

    with engine.connect() as con:
        total = con.execute(text(count_sql), params).scalar_one()
        df = pd.read_sql(
            text(data_sql), con,
            params={**params, "lim": limit, "off": offset}
        )

    return df, int(total)


# =========================
# Operaciones CRUD básicas
# =========================
def insert_row(engine: Engine, table: str, schema: str, values: Dict[str, Any]) -> None:
    """
    Inserta un registro. No incluye la PK si es autoincremental (el caller debe omitirla).
    """
    # Filtra claves None para columnas opcionales (puedes cambiar esta lógica si quieres forzar NULL explícito)
    to_insert = {k: v for k, v in values.items()}
    if not to_insert:
        return

    cols = ",".join(f'"{k}"' for k in to_insert.keys())
    ph   = ",".join(f":{k}" for k in to_insert.keys())
    sql  = f'INSERT INTO "{schema}"."{table}" ({cols}) VALUES ({ph})'

    with engine.begin() as con:
        con.execute(text(sql), to_insert)


def update_row(engine: Engine, table: str, schema: str, pk: str, pk_value: Any, values: Dict[str, Any]) -> None:
    """
    Actualiza un registro por PK. Excluye la PK del set.
    """
    to_update = {k: v for k, v in values.items() if k != pk}
    if not to_update:
        return

    sets = ",".join(f'"{k}" = :{k}' for k in to_update.keys())
    sql  = f'UPDATE "{schema}"."{table}" SET {sets} WHERE "{pk}" = :_pk_val'

    with engine.begin() as con:
        con.execute(text(sql), {**to_update, "_pk_val": pk_value})


def delete_row(engine: Engine, table: str, schema: str, pk: str, pk_value: Any) -> None:
    """
    Borra un registro por PK.
    """
    sql = f'DELETE FROM "{schema}"."{table}" WHERE "{pk}" = :_pk_val'
    with engine.begin() as con:
        con.execute(text(sql), {"_pk_val": pk_value})


# =========================
# Mapeo de tipos SQL → widgets básicos
# =========================
def default_widget_value(sql_type: str) -> str:
    """
    Mapea el tipo SQL (de information_schema) a un 'tipo de widget' simple:
      - "number": ints, numerics, double precision, real, decimal
      - "bool": boolean
      - "date": date, timestamp, time
      - "text": por defecto
    """
    t = (sql_type or "").lower()
    if any(k in t for k in ["int", "numeric", "double", "real", "decimal"]):
        return "number"
    if "bool" in t:
        return "bool"
    if "date" in t or "time" in t:
        return "date"
    return "text"
