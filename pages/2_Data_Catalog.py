# encoding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 30 10:28:58 2025
@author: ngoyenaserv
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
import sys, os, importlib.util, traceback

# ========================================
# Import robusto de db_utils desde raíz
# ========================================

# Calcular ruta absoluta del proyecto (un nivel arriba de /pages)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
db_utils_path = os.path.join(project_root, "db_utils.py")

if os.path.exists(db_utils_path):
    spec = importlib.util.spec_from_file_location("db_utils", db_utils_path)
    db_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(db_utils)
    get_engine = db_utils.get_engine
    infer_pk = db_utils.infer_pk
else:
    st.error(f"❌ No se encontró db_utils.py en {project_root}")
    st.stop()



# =========================
# CACHÉ Y OPTIMIZACIÓN
# =========================

@st.cache_resource
def get_cached_engine() -> Engine:
    """Mantiene viva la conexión SQLAlchemy durante toda la sesión."""
    return get_engine()

@st.cache_data(ttl=600)
def get_cached_columns(_engine: Engine, table: str):
    """Devuelve columnas de una tabla (cacheadas 10 min)."""
    insp = inspect(_engine)
    return insp.get_columns(table, schema="public")

@st.cache_data(ttl=600)
def count_cached_records(_engine: Engine, table: str, where: str, params: Dict[str, Any]) -> int:
    sql = f'SELECT COUNT(*) FROM "{table}"{where}'
    with _engine.connect() as con:
        c = con.execute(text(sql), params).scalar()
    return int(c or 0)

@st.cache_data(ttl=60)
def fetch_cached_records(_engine: Engine, table: str, where: str, params: Dict[str, Any], order_col: str, limit: int, offset: int):
    sql = f'SELECT * FROM "{table}"{where} ORDER BY "{order_col}" DESC LIMIT :_lim OFFSET :_off'
    p = dict(params)
    p["_lim"] = limit
    p["_off"] = offset
    with _engine.connect() as con:
        df = pd.read_sql(text(sql), con, params=p)
    return df


# =========================
# Utilidades
# =========================

def normalize_drive_url(url: str) -> str:
    """Normaliza URLs de Drive a formato directo (uc?id=...)."""
    if not isinstance(url, str) or not url:
        return ""
    u = url.strip()
    if "drive.google.com/uc?id=" in u:
        return u
    if "drive.google.com/file/d/" in u:
        try:
            file_id = u.split("/file/d/")[1].split("/")[0]
            return f"https://drive.google.com/uc?id={file_id}"
        except Exception:
            return u
    return u

def python_value_for_sql(val):
    if isinstance(val, (date, datetime)):
        return val
    if val == "":
        return None
    return val

def is_textual(coltype: str) -> bool:
    c = coltype.lower()
    return any(x in c for x in ["char", "text", "json", "uuid"])

def is_numeric(coltype: str) -> bool:
    c = coltype.lower()
    return any(x in c for x in ["int", "numeric", "float", "double", "real", "decimal"])

def is_temporal(coltype: str) -> bool:
    c = coltype.lower()
    return any(x in c for x in ["date", "time"])

def pick_display_fields(cols: List[Dict[str, Any]]) -> List[str]:
    names = [c["name"] for c in cols]
    priority = ["name","title","sample_id","reservoir","reservoir_name","point_name","type","category","date","created_at"]
    chosen = [c for c in priority if c in names]
    for n in names:
        if n not in chosen and len(chosen) < 5:
            chosen.append(n)
    return chosen[:5]

def choose_order_column(cols: List[Dict[str, Any]], pk: Optional[str]) -> str:
    if pk:
        return pk
    candidates = ["updated_at", "created_at", "timestamp", "ts", "date"]
    names = [c["name"] for c in cols]
    for c in candidates:
        if c in names:
            return c
    return names[0] if names else "1"

from streamlit_folium import folium_static
import folium

def get_extraction_point_coords(engine, extraction_point_id):
    """
    Dado un extraction_point_id, devuelve las coordenadas (lat, lon)
    del punto de extracción desde la tabla extraction_points.
    """
    try:
        sql = text("""
            SELECT latitude, longitude
            FROM extraction_points
            WHERE extraction_point_id = :eid
            LIMIT 1
        """)
        with engine.connect() as con:
            row = con.execute(sql, {"eid": extraction_point_id}).fetchone()
        if row and row[0] is not None and row[1] is not None:
            return float(row[0]), float(row[1])
    except Exception as e:
        st.error(f"❌ Error obteniendo coordenadas: {e}")
    return None




# =========================
# CRUD helpers
# =========================

def get_record_by_id(engine: Engine, table: str, pk: str, pk_value: Any) -> Optional[pd.Series]:
    sql = f'SELECT * FROM "{table}" WHERE "{pk}" = :id'
    with engine.connect() as con:
        df = pd.read_sql(text(sql), con, params={"id": pk_value})
    if df.empty:
        return None
    return df.iloc[0]

def insert_record(engine: Engine, table: str, data: Dict[str, Any]):
    cols = ", ".join(f'"{k}"' for k in data.keys())
    vals = ", ".join(f":{k}" for k in data.keys())
    sql = f'INSERT INTO "{table}" ({cols}) VALUES ({vals})'
    with engine.begin() as con:
        con.execute(text(sql), {k: python_value_for_sql(v) for k, v in data.items()})

def update_record(engine: Engine, table: str, pk: str, pk_value: Any, data: Dict[str, Any]):
    sets = ", ".join(f'"{k}" = :{k}' for k in data.keys())
    sql = f'UPDATE "{table}" SET {sets} WHERE "{pk}" = :_pkval'
    params = {k: python_value_for_sql(v) for k, v in data.items()}
    params["_pkval"] = pk_value
    with engine.begin() as con:
        con.execute(text(sql), params)

def delete_record(engine: Engine, table: str, pk: str, pk_value: Any):
    sql = f'DELETE FROM "{table}" WHERE "{pk}" = :_pkval'
    with engine.begin() as con:
        con.execute(text(sql), {"_pkval": pk_value})

def render_input_for_column(colmeta: Dict[str, Any], default=None):
    label = colmeta["name"]
    ctype = str(colmeta.get("type", ""))
    if is_temporal(ctype):
        if "time" in ctype:
            return st.datetime_input(label, value=default if isinstance(default, datetime) else None, format="DD-MM-YYYY HH:mm")
        else:
            return st.date_input(label, value=default if isinstance(default, date) else None, format="DD-MM-YYYY")
    elif is_numeric(ctype):
        return st.number_input(label, value=float(default) if default not in (None, "") else 0.0, step=1.0)
    elif "bool" in ctype.lower():
        return st.checkbox(label, value=bool(default) if default is not None else False)
    else:
        if "text" in ctype.lower() or "json" in ctype.lower():
            return st.text_area(label, value=str(default or ""))
        return st.text_input(label, value=str(default or ""))

# ======================
# Cabecera compacta con controles arriba a la derecha
# ======================

st.set_page_config(page_title="Catálogo HIBLOOMS", layout="wide")

# CSS para compactar el área superior y bloquear escritura en el selectbox
st.markdown(
    """
    <style>
    h1, h2, h3 {
        margin-top: 0rem !important;
        margin-bottom: 0.3rem !important;
    }
    div[data-testid="stHorizontalBlock"] {
        margin-top: -1rem !important;
        margin-bottom: -0.5rem !important;
    }
    section[data-testid="stVerticalBlock"] > div {
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }

    /* 🔒 Bloquear escritura en el selectbox */
    div[data-baseweb="select"] input {
        pointer-events: none !important;
        caret-color: transparent !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# Conexión y carga de tablas
try:
    engine = get_cached_engine()
    insp = inspect(engine)
    all_tables = [t for t in insp.get_table_names(schema="public") if t.lower() != "spatial_ref_sys"]
except Exception as e:
    st.error(f"❌ Error obteniendo conexión o lista de tablas: {e}")
    st.stop()

# Diccionario de nombres amigables
TABLE_LABELS = {
    "reservoirs_spain": "🏞️ Embalses de España",
    "extraction_points": "📍 Puntos de extracción",
    "lab_images": "🧫 Imágenes de laboratorio",
    "insitu_sampling": "🧪 Muestreos in situ",
    "profiles_data": "🌡️ Perfiles de datos",
    "sediment_data": "🪨 Datos de sedimentos",
    "insitu_determinations": "🔬 Determinaciones in situ",
    "rivers_spain": "🌊 Ríos de España",
    "sensor_data": "📈 Datos de sensores",
    "samples": "🧫 Muestras de laboratorio",
}
# Diccionario de nombres de columnas traducidos al español
COLUMN_LABELS = {
    # --- General ---
    "extraction_id": "ID de extracción",
    "extraction_point_id": "ID del punto de extracción",
    "reservoir_id": "ID del embalse",
    "river_id": "ID del río",
    "reservoir_name": "Nombre del embalse",
    "river_name": "Nombre del río",
    "water_body_name": "Nombre del cuerpo de agua",
    "latitude": "Latitud",
    "longitude": "Longitud",
    "geometry": "Geometría",
    "date_time": "Fecha y hora",
    "datetime": "Fecha y hora",
    "date": "Fecha",
    "time": "Hora",
    "depth": "Profundidad (m)",
    "depth_m": "Profundidad (m)",

    # --- extraction_points ---
    "location_code": "Código de localización",
    "sampling_instance_per_location": "Instancia de muestreo por localización",

    # --- all_samples / samples ---
    "sample_type": "Tipo de muestra",
    "sample_date": "Fecha de muestra",
    "sample_time": "Hora de muestra",

    # --- sensor_data ---
    "chlorophyll": "Clorofila (µg/L)",
    "phycocyanin": "Ficocianina (µg/L)",
    "water_temp": "Temperatura del agua (°C)",
    "ph": "pH",
    "turbidity": "Turbidez (NTU)",

    # --- insitu_sampling ---
    "climate_description": "Descripción del clima",
    "water_temperature_celsius": "Temperatura del agua (°C)",
    "water_ph": "pH del agua",
    "water_depth_meters": "Profundidad del agua (m)",
    "secchi_depth_meters": "Profundidad Secchi (m)",
    "water_transparency_percent": "Transparencia del agua (%)",
    "electrical_conductivity_us_cm": "Conductividad eléctrica (µS/cm)",
    "dissolved_oxygen_percent_1": "Oxígeno disuelto 1 (%)",
    "dissolved_oxygen_percent_2": "Oxígeno disuelto 2 (%)",
    "chlorophyll_volume_estimation": "Estimación de volumen de clorofila",
    "chlorophyll_a_mg_m3": "Clorofila a (mg/m³)",
    "chlorophyll_b_mg_m3": "Clorofila b (mg/m³)",
    "chlorophyll_c_mg_m3": "Clorofila c (mg/m³)",
    "alkalinity_meq_l": "Alcalinidad (meq/L)",
    "chloride_ion_mg_l": "Ion cloruro (mg/L)",
    "nitrite_no2_mg_l": "Nitrito (mg/L)",
    "nitrate_no3_mg_l": "Nitrato (mg/L)",
    "sulfate_so4_mg_l": "Sulfato (mg/L)",
    "ammonium_nh4_mg_l": "Amonio (mg/L)",
    "npoc_mg_l": "NPOC (mg/L)",
    "total_nitrogen_wstn_mg_l": "Nitrógeno total (mg/L)",
    "elisa_cylindrospermopsin_ng_ml": "Cilindrospermopsina (ng/mL)",
    "elisa_microcystin_nodularin_ng_ml": "Microcistina/Nodularina (ng/mL)",

    # --- insitu_sampling: metales ---
    "beryllium_ppm": "Berilio (ppm)",
    "boron_ppm": "Boro (ppm)",
    "sodium_ppm": "Sodio (ppm)",
    "magnesium_ppm": "Magnesio (ppm)",
    "aluminum_ppm": "Aluminio (ppm)",
    "silicon_ppm": "Silicio (ppm)",
    "phosphorus_ppm": "Fósforo (ppm)",
    "sulfur_34_ppm": "Azufre (ppm)",
    "potassium_ppm": "Potasio (ppm)",
    "calcium_44_ppm": "Calcio (ppm)",
    "titanium_ppm": "Titanio (ppm)",
    "vanadium_ppm": "Vanadio (ppm)",
    "chromium_ppm": "Cromo (ppm)",
    "manganese_ppm": "Manganeso (ppm)",
    "iron_ppm": "Hierro (ppm)",
    "cobalt_ppm": "Cobalto (ppm)",
    "nickel_ppm": "Níquel (ppm)",
    "copper_ppm": "Cobre (ppm)",
    "zinc_ppm": "Zinc (ppm)",
    "gallium_ppm": "Galio (ppm)",
    "germanium_ppm": "Germanio (ppm)",
    "arsenic_ppm": "Arsénico (ppm)",
    "selenium_ppm": "Selenio (ppm)",
    "rubidium_ppm": "Rubidio (ppm)",
    "strontium_ppm": "Estroncio (ppm)",
    "zirconium_ppm": "Zirconio (ppm)",
    "niobium_ppm": "Niobio (ppm)",
    "molybdenum_ppm": "Molibdeno (ppm)",
    "silver_ppm": "Plata (ppm)",
    "cadmium_ppm": "Cadmio (ppm)",
    "tin_ppm": "Estaño (ppm)",
    "antimony_ppm": "Antimonio (ppm)",
    "tellurium_ppm": "Telurio (ppm)",
    "barium_ppm": "Bario (ppm)",

    # --- insitu_determinations ---
    "date_sampling": "Fecha de muestreo",
    "time_sampling": "Hora de muestreo",
    "probe": "Sonda",
    "lote_medida": "Lote de medida",
    "do_percent": "Oxígeno disuelto (%)",
    "do_ppm": "Oxígeno disuelto (ppm)",
    "conduc_uscm_1": "Conductividad (µS/cm) 1",
    "conduc_uscm_2": "Conductividad (µS/cm) 2",
    "temp": "Temperatura (°C)",

    # --- profiles_data ---
    "green_algae_ug_l": "Algas verdes (µg/L)",
    "bluegreen_ug_l": "Cianobacterias (µg/L)",
    "diatoms_ug_l": "Diatomeas (µg/L)",
    "cryptophyta_ug_l": "Criptófitas (µg/L)",
    "yellow_substances_ru": "Sustancias amarillas (RU)",
    "total_concentration_ug_l": "Concentración total (µg/L)",
    "transmission_percent": "Transmisión (%)",
    "sample_temperature_celsius": "Temperatura de la muestra (°C)",

    # --- sediment_data ---
    "sampling_date": "Fecha de muestreo",
    "treatment_date": "Fecha de tratamiento",
    "plate_code": "Código de placa",
    "drying_temperature": "Temperatura de secado (°C)",
    "crisol_code": "Código del crisol",
    "crucible_tare": "Tara del crisol (g)",
    "sample_01_tare": "Tara muestra 01 (g)",
    "date_02": "Fecha 02",
    "sample_02_tare": "Tara muestra 02 (g)",
    "date_03": "Fecha 03",
    "sample_03_tare": "Tara muestra 03 (g)",
    "date_04": "Fecha 04",
    "sample_04_tare": "Tara muestra 04 (g)",
    "humidity": "Humedad (%)",
    "date_ppi": "Fecha PPI",
    "sample_105_tare": "Tara muestra 105°C (g)",
    "ppi_temperature": "Temperatura PPI (°C)",
    "sample_550_tare": "Tara muestra 550°C (g)",
    "tara_sample_550": "Tara muestra + crisol (550°C)",
    "ppi": "Pérdida por ignición (%)",
    "co": "Carbono orgánico (%)",
    "observations": "Observaciones",

    # --- reservoirs_spain ---
    "report_url": "URL del informe",
    "capacity_nmn": "Capacidad (hm³)",
    "elevation_nmn": "Elevación (m)",
    "owner": "Propietario",
    "managing_authority": "Autoridad gestora",
    "river_basin_district": "Cuenca hidrográfica",
    "province": "Provincia",
    "basin_area_km2": "Área de cuenca (km²)",
    "annual_precip_mm": "Precipitación anual (mm)",
    "reservoir_type": "Tipo de embalse",
    "responsible_operator": "Operador responsable",
    "ownership_type": "Tipo de propiedad",
    "use_purpose": "Uso principal",
    "area_m2": "Superficie (m²)",

    # --- rivers_spain ---
    "length": "Longitud (m)",

    # --- lab_images ---
    "image_id": "ID de imagen",
    "image_name": "Nombre de la imagen",
    "image_url": "URL de la imagen",
    "description": "Descripción",
    "date_captured": "Fecha de captura",
    "uploaded_at": "Fecha de subida",
    "photographer": "Fotógrafo",
    "notes": "Notas",
}



# Layout compacto: título a la izquierda, selector a la derecha
col_title, col_controls = st.columns([3, 2])

with col_title:
    st.markdown("## 📘 Catálogo HIBLOOMS")

with col_controls:
    st.markdown("#### ⚙️ Consultar registros sobre:")
    table_options = [TABLE_LABELS.get(t, t) for t in all_tables]
    selected_label = st.selectbox("Selecciona una tabla", table_options, index=0, label_visibility="collapsed")
    table = next(k for k, v in TABLE_LABELS.items() if v == selected_label)

# Parámetros por defecto
page_size = 100
page = st.session_state.get("page", 1)

# Línea divisoria sutil
st.markdown("<hr style='margin-top:-0.2rem; margin-bottom:0.8rem;'>", unsafe_allow_html=True)





# =========================
# SUBPÁGINAS DE DETALLE (todas las tablas)
# =========================
params = st.query_params

# ====== Detalle de un registro (genérico o lab_images) ======
if "page" in params and params.get("page") in ["lab_image", "detail"] and "id" in params:
    table = params.get("table", "lab_images") if params.get("page") == "detail" else "lab_images"
    record_id = params.get("id")

    cols = get_cached_columns(engine, table)
    pk = infer_pk(engine, table) or cols[0]["name"]

    row = get_record_by_id(engine, table, pk, record_id)
    if row is None:
        st.error("❌ No se encontró el registro solicitado.")
        st.stop()

    # Encabezado del detalle
    st.subheader(f"📄 Detalle del registro (tabla: {table}, ID: {record_id})")

    # =============================
    # CASO ESPECIAL: lab_images → imagen + mapa
    # =============================
    if table == "lab_images":
        st.markdown(
            """
            <h3 style='text-align:center; margin-bottom:12px;'>🧫 Imagen de laboratorio</h3>
            """,
            unsafe_allow_html=True
        )
        img_url = normalize_drive_url(str(row.get("image_url", "")))
        if img_url:
            proxy_url = f"https://images.weserv.nl/?url={img_url.replace('https://', '')}"
            st.markdown(
                f"""
                <div style="display:flex; justify-content:center; align-items:center;">
                    <img src="{proxy_url}" alt="Imagen de laboratorio" style="
                        max-width: 55%;
                        max-height: 350px;
                        height: auto;
                        object-fit: contain;
                        border-radius: 10px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                    ">
                </div>
                <p style='text-align:center; color:#666;'>ID {record_id}</p>
                """,
                unsafe_allow_html=True
            )

        # Mapa si existe extraction_id
        if "extraction_id" in row and pd.notna(row["extraction_id"]):
            coords = get_extraction_point_coords(engine, row["extraction_id"])
            if coords:
                lat, lon = coords
                st.markdown("<h3 style='text-align:left;'>🗺️ Punto de extracción asociado</h3>", unsafe_allow_html=True)
                m = folium.Map(location=[lat, lon], zoom_start=15, tiles="Esri.WorldImagery")
                folium.Marker([lat, lon], tooltip="Punto de extracción", icon=folium.Icon(color="red")).add_to(m)
                folium_static(m, width=700, height=400)

    # =============================
    # Información general
    # =============================
    st.markdown("### 📋 Información del registro")
    df_meta = pd.DataFrame(row).reset_index()
    df_meta.columns = ["Campo", "Valor"]
    df_display = df_meta.rename(columns=COLUMN_LABELS)
    st.dataframe(df_display, use_container_width=True, hide_index=True)



    # =============================
    # Mostrar mapa si existe extraction_point_id
    # =============================
    if "extraction_point_id" in row and pd.notna(row["extraction_point_id"]):
        coords = get_extraction_point_coords(engine, row["extraction_point_id"])
        if coords:
            lat, lon = coords
            st.markdown("<h3 style='text-align:center;'>🗺️ Ubicación del punto de extracción</h3>", unsafe_allow_html=True)
            m = folium.Map(location=[lat, lon], zoom_start=13, tiles="Esri.WorldImagery")
            folium.Marker(
                [lat, lon],
                tooltip=f"Punto de extracción {row['extraction_point_id']}",
                icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(m)
            folium_static(m, width=700, height=400)


    # =============================
    # Edición del registro
    # =============================
    st.markdown("---")
    edit_mode = st.toggle("✏️ Editar registro", value=False)
    if edit_mode:
        with st.form("form_edit_generic", clear_on_submit=False):
            new_values = {}
            for c in cols:
                cname = c["name"]
                if cname == pk:
                    st.text_input(cname, value=str(row.get(cname)), disabled=True)
                else:
                    new_values[cname] = render_input_for_column(c, default=row.get(cname))
            if st.form_submit_button("Guardar cambios"):
                update_record(engine, table, pk, record_id, new_values)
                st.success("✅ Cambios guardados.")
                st.query_params.update(page="detail", table=table, id=record_id)
                st.rerun()

    # =============================
    # Botones inferiores
    # =============================
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("⬅️ Volver al catálogo"):
            current_table = st.query_params.get("table", "lab_images")
            current_page = st.session_state.get("page", 1)
            st.query_params.clear()
            st.query_params.update(table=current_table)
            st.session_state["page"] = current_page
            st.rerun()


    with col2:
        if st.button("🗑️ Borrar registro"):
            delete_record(engine, table, pk, record_id)
            st.success("✅ Registro eliminado.")
            st.query_params.clear()
            st.rerun()

    st.stop()

# ====== Detalle de un grupo de muestras ======
if params.get("page") == "detail" and "group" in params and "time" in params:
    table = params.get("table")
    point_id = params.get("group")
    time_group = params.get("time")

    # Caso especial: sensor_data (usa sensor_type en lugar de fecha)
    if table == "sensor_data":
        st.subheader(f"📊 Detalle de grupo — {point_id} · {time_group}")
        sql = text("""
            SELECT * FROM sensor_data
            WHERE reservoir_name = :res AND 
                  ((:stype = 'Clorofila' AND phycocyanin IS NULL AND chlorophyll IS NOT NULL)
                   OR (:stype = 'Ficocianina' AND chlorophyll IS NULL AND phycocyanin IS NOT NULL))
            ORDER BY date_time ASC
        """)
        with engine.connect() as con:
            df_group = pd.read_sql(sql, con, params={"res": point_id, "stype": time_group})

        # Mostrar mapa del embalse si existe
        st.markdown("### 🗺️ Ubicación del embalse (si aplica)")
        coords = get_extraction_point_coords(engine, None)  # opcional, podrías enlazar a otra tabla
        # Aplicar traducción de nombres de columnas
        df_display = df_group.rename(columns=COLUMN_LABELS)
        st.dataframe(df_display, use_container_width=True, hide_index=True)



        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⬅️ Volver al catálogo"):
                current_table = st.query_params.get("table", table)
                current_page = st.session_state.get("page", 1)
                st.query_params.clear()
                st.query_params.update(table=current_table)
                st.session_state["page"] = current_page
                st.rerun()

        with col2:
            st.info("🧩 Eliminación de grupos de sensores aún no implementada.")

        st.stop()

    # === Resto de tablas ===
    table_cols = [c["name"] for c in get_cached_columns(engine, table)]
    time_candidates = ["datetime", "date_time", "date_sampling", "sample_date", "timestamp", "created_at", "date"]
    time_col = next((c for c in time_candidates if c in table_cols), None)

    if not time_col:
        st.error(f"❌ No se encontró una columna temporal adecuada en la tabla '{table}'. Columnas disponibles: {table_cols}")
        st.stop()

    # Parsear el valor de hora recibido (solo para tablas con fecha real)
    try:
        time_start = pd.to_datetime(time_group)
    except Exception as e:
        st.error(f"⚠️ Error interpretando el parámetro de tiempo: {e}")
        st.stop()


    # Usar un rango temporal más amplio (±2 horas)
    time_start_range = time_start - pd.Timedelta(hours=2)
    time_end_range = time_start + pd.Timedelta(hours=2)

    sql = text(f"""
        SELECT * FROM "{table}"
        WHERE extraction_point_id = :pid
          AND "{time_col}" BETWEEN :tstart AND :tend
        ORDER BY "{time_col}" ASC
    """)

    with engine.connect() as con:
        df_group = pd.read_sql(sql, con, params={
            "pid": point_id,
            "tstart": time_start_range,
            "tend": time_end_range
        })

    st.subheader(f"📊 Detalle de grupo — Punto {point_id}, {time_start.strftime('%Y-%m-%d %H:%M')} (±2 h)")
    # =============================
    # Mapa del punto de extracción (vista de grupo)
    # =============================
    if point_id:
        coords = get_extraction_point_coords(engine, point_id)
        if coords:
            lat, lon = coords
            st.markdown("### 🗺️ Ubicación del punto de extracción")
            m = folium.Map(location=[lat, lon], zoom_start=13, tiles="Esri.WorldImagery")
            folium.Marker(
                [lat, lon],
                tooltip=f"Punto de extracción {point_id}",
                icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(m)
            folium_static(m, width=700, height=400)

    if df_group.empty:
        st.warning("⚠️ No se encontraron registros en el rango temporal especificado (±2 h).")

        # 🔍 Debug temporal: mostrar registros reales de ese punto
        st.markdown("### 🔍 Depuración de registros disponibles")
        with engine.connect() as con:
            df_check = pd.read_sql(text(f"""
                SELECT "{time_col}", extraction_point_id
                FROM "{table}"
                WHERE extraction_point_id = :pid
                ORDER BY "{time_col}" ASC
                LIMIT 15
            """), con, params={"pid": point_id})
        if df_check.empty:
            st.info("No hay registros para este punto de extracción.")
        else:
            # Aplicar traducción de nombres de columnas
            df_display = df_check.rename(columns=COLUMN_LABELS)
            st.dataframe(df_display, use_container_width=True, hide_index=True)




            st.caption(f"Mostrando las 15 primeras fechas del punto {point_id} (columna '{time_col}').")

    else:
        # Aplicar traducción de nombres de columnas
        df_display = df_group.rename(columns=COLUMN_LABELS)
        st.dataframe(df_display, use_container_width=True, hide_index=True)



    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("⬅️ Volver al catálogo"):
            current_table = st.query_params.get("table", table)
            current_page = st.session_state.get("page", 1)
            st.query_params.clear()
            st.query_params.update(table=current_table)
            st.session_state["page"] = current_page
            st.rerun()
    
    with col2:
        if st.button("🗑️ Borrar grupo"):
            st.warning("⚠️ Eliminación de grupos completa aún no implementada.")

    st.stop()




# Cachear columnas y metadatos
if "cols_cache" not in st.session_state:
    st.session_state["cols_cache"] = {}

if table not in st.session_state["cols_cache"]:
    st.session_state["cols_cache"][table] = get_cached_columns(engine, table)

cols = st.session_state["cols_cache"][table]
pk = infer_pk(engine, table) or (cols[0]["name"] if cols else None)
order_col = choose_order_column(cols, pk)

where, params_sql = "", {}
offset = (page - 1) * page_size
total = count_cached_records(engine, table, where, params_sql)
if table in ["sensor_data", "sediment_data", "insitu_determinations", "insitu_sampling", "samples", "profiles_data"]:
    # 🔁 Leer todos los registros para agrupar correctamente
    with engine.connect() as con:
        df = pd.read_sql(text(f'SELECT * FROM "{table}"'), con)
else:
    # 🔁 Mantener la carga paginada para tablas grandes
    df = fetch_cached_records(engine, table, where, params_sql, order_col, page_size, offset)


# ===== Vista especial lab_images =====
if table == "lab_images":
    st.markdown("### 🖼️ Galería de imágenes (clic para ver detalle)")
    st.markdown("<div style='display:flex; flex-wrap:wrap; gap:20px; justify-content:center;'>", unsafe_allow_html=True)

    n_cols = 4
    rows_chunks = [df.iloc[i:i+n_cols] for i in range(0, len(df), n_cols)]
    for chunk in rows_chunks:
        cols_ui = st.columns(n_cols, gap="large")
        for (ridx, rrow), col_ui in zip(chunk.iterrows(), cols_ui):
            with col_ui:
                img_url = normalize_drive_url(str(rrow.get("image_url", "")))
                proxy_url = f"https://images.weserv.nl/?url={img_url.replace('https://', '')}" if img_url else ""
                extraction_id = rrow.get("extraction_id", "(sin extraction_id)")
                record_id = rrow.get(pk)

                # Enlace directo clicable en toda la tarjeta
                detail_url = f"?page=lab_image&id={record_id}"

                st.markdown(
                    f"""
                    <a href="{detail_url}" style="text-decoration:none; color:inherit;">
                        <div style="
                            text-align:center;
                            border:1px solid #ccc;
                            border-radius:10px;
                            padding:10px;
                            background:#fff;
                            transition:all 0.2s ease-in-out;
                            box-shadow:0 2px 6px rgba(0,0,0,0.08);
                        " 
                        onmouseover="this.style.boxShadow='0 4px 10px rgba(0,0,0,0.15)'; this.style.transform='scale(1.02)';"
                        onmouseout="this.style.boxShadow='0 2px 6px rgba(0,0,0,0.08)'; this.style.transform='scale(1)';">
                            {"<img src='" + proxy_url + "' style='max-width:100%; height:auto; border-radius:8px;'>" if proxy_url else "<p>⚠️ Sin imagen</p>"}
                            <p style="font-weight:600; margin-top:6px;">🧪 Extraction ID: <span style="color:#1e88e5;">{extraction_id}</span></p>
                            <p style="color:#666;">ID {record_id}</p>
                        </div>
                    </a>
                    """,
                    unsafe_allow_html=True
                )

    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# ===== TABLAS NORMALES O AGRUPADAS =====

# Calcular índice global (no reiniciado por página)
df.index = df.index + 1 + offset

# Tablas que deben mostrarse agrupadas
grouped_tables = ["samples", "profiles_data", "insitu_determinations", "insitu_sampling", "sediment_data", "sensor_data"]

if table in grouped_tables:
    st.markdown(f"### 🧩 Registros agrupados de `{table}` por punto y hora/fecha")

    if df.empty:
        st.info("No se han encontrado registros.")
    else:
        # Detección automática de columna temporal
        table_cols = [col["name"] for col in get_cached_columns(engine, table)]
        time_col = next(
            (c for c in ["datetime", "date_time", "date", "created_at", "timestamp", "sample_date"]
             if c in table_cols),
            None
        )

        # Caso especial para insitu_sampling: agrupar solo por fecha y punto
        if table == "insitu_sampling":
            grouped = df.groupby(["extraction_point_id", "sample_date"])
        elif table == "sediment_data":
            grouped = df.groupby(["extraction_point_id", "sampling_date"])
        elif table == "insitu_determinations":
            grouped = df.groupby(["extraction_point_id", "date_sampling", "time_sampling"])
        elif table == "sensor_data":
            # Clasificar tipo de sensor según las columnas activas
            df["sensor_type"] = df.apply(
                lambda r: "Clorofila" if pd.notna(r.get("chlorophyll")) else (
                    "Ficocianina" if pd.notna(r.get("phycocyanin")) else "Sin datos"
                ),
                axis=1
            )
            grouped = df.groupby(["reservoir_name", "sensor_type"])

        else:
            if not time_col:
                st.warning("No se encontró columna temporal para agrupar.")
                st.stop()
            # Agrupar por punto y hora redondeada
            df["hour_group"] = pd.to_datetime(df[time_col]).dt.floor("H")
            grouped = df.groupby(["extraction_point_id", "hour_group"])

        # Paginación por grupos (no por filas)
        start_idx = offset
        end_idx = offset + page_size
        visible_groups = list(grouped)[start_idx:end_idx]

        for keys, group in visible_groups:
            with st.container(border=True):
                # Desempaquetar claves del grupo según el número de columnas agrupadas
                if table == "insitu_determinations":
                    point_id, date_sampling, time_sampling = keys
                elif table == "sediment_data":
                    point_id, group_time = keys
                elif table == "insitu_sampling":
                    point_id, group_time = keys
                elif table == "sensor_data":
                    reservoir_name, sensor_type = keys
                else:
                    point_id, group_time = keys
        
                # Obtener nombre del embalse si existe
                if table == "sensor_data":
                    titulo = f"📈 {reservoir_name} — {sensor_type}"
                else:
                    reservoir_name_val = None
                    if "reservoir_name" in group.columns:
                        val = group["reservoir_name"].dropna().unique()
                        if len(val) > 0:
                            reservoir_name_val = str(val[0])
                    titulo = f"📍 {reservoir_name_val} – Punto {point_id}" if reservoir_name_val else f"📍 Punto {point_id}"
        
                # Mostrar encabezado según tipo de tabla
                if table == "insitu_determinations":
                    fecha_str = str(date_sampling) if pd.notna(date_sampling) else "(sin fecha)"
                    hora_str = str(time_sampling) if pd.notna(time_sampling) else "(sin hora)"
                    st.markdown(f"#### {titulo} — {fecha_str} {hora_str}")
                    detail_url = f"?page=detail&table={table}&group={point_id}&time={fecha_str}T{hora_str}"
                elif table in ["insitu_sampling", "sediment_data"]:
                    st.markdown(f"#### {titulo} — Fecha {group_time}")
                    detail_url = f"?page=detail&table={table}&group={point_id}&time={group_time}"
                elif table == "sensor_data":
                    st.markdown(f"#### {titulo}")
                    detail_url = f"?page=detail&table={table}&group={reservoir_name}&time={sensor_type}"
                    if "sensor_type" in group.columns:
                        group = group.drop(columns=["sensor_type"])
                else:
                    st.markdown(f"#### {titulo} — {group_time.strftime('%Y-%m-%d %H:%M')}")
                    detail_url = f"?page=detail&table={table}&group={point_id}&time={group_time.isoformat()}"



                # Mostrar las 5 primeras filas del grupo
                df_display = group.head(5).rename(columns=COLUMN_LABELS)
                st.dataframe(df_display, hide_index=True, use_container_width=True)


                st.markdown(
                    f"""
                    <a href="{detail_url}" target="_self" style="text-decoration:none;">
                        <button style="
                            background-color:#1e88e5;
                            color:white;
                            border:none;
                            padding:6px 12px;
                            border-radius:6px;
                            cursor:pointer;
                            margin-top:8px;">
                            🔎 Ver detalles
                        </button>
                    </a>
                    """,
                    unsafe_allow_html=True
                )

        # Calcular número de grupos y páginas
        total_groups = len(grouped)
        total_pages = max(1, (total_groups + page_size - 1) // page_size)
        st.caption(f"Mostrando {len(visible_groups)} grupos (Página {page} de {total_pages})")

else:
    if table == "reservoirs_spain":
        st.markdown("### 🗺️ Mapa de embalses por cuenca hidrográfica y selección individual")
    
        import geopandas as gpd
        from shapely import wkb
        import binascii
        from streamlit_folium import folium_static
        import folium
        from sqlalchemy import text
    
        # --- Leer todos los embalses directamente desde la BD ---
        with engine.connect() as con:
            df_full = pd.read_sql(text('SELECT * FROM "reservoirs_spain"'), con)
        st.write("📂 Tabla cargada:", "reservoirs_spain")
        st.write("Registros leídos:", len(df_full))
        st.write("Área mínima:", df_full["area_m2"].min())

    
        # --- Función para convertir geometrías WKB hex a shapely ---
        def safe_load_wkb_hex(geom):
            try:
                if geom is None:
                    return None
                if isinstance(geom, str):
                    geom = geom.strip()
                    if geom.startswith("01") and all(c in "0123456789ABCDEFabcdef" for c in geom[:50]):
                        return wkb.loads(binascii.unhexlify(geom))
                elif isinstance(geom, (bytes, bytearray, memoryview)):
                    return wkb.loads(bytes(geom))
            except Exception:
                return None
            return None
    
        df_full["geometry"] = df_full["geometry"].apply(safe_load_wkb_hex)
        df_full = df_full[df_full["geometry"].notnull()]
    
        if df_full.empty:
            st.warning("⚠️ No se pudo leer ninguna geometría válida.")
            st.stop()
    
        # --- Selección de cuenca hidrográfica ---
        cuencas = sorted(df_full["river_basin_district"].dropna().unique().tolist())
        selected_cuenca = st.selectbox("🌊 Selecciona una cuenca hidrográfica:", cuencas)
    
        df_cuenca = df_full[df_full["river_basin_district"] == selected_cuenca].copy()
        if df_cuenca.empty:
            st.warning("No hay embalses en esta cuenca.")
            st.stop()
    
        # --- Selector predictivo de embalse dentro de la cuenca ---
        embalses = sorted(df_cuenca["reservoir_name"].dropna().unique().tolist())
        selected_embalse = st.selectbox("🏞️ Selecciona un embalse:", embalses, index=None, placeholder="Escribe un nombre...")
    
        if selected_embalse:
            df_sel = df_cuenca[df_cuenca["reservoir_name"] == selected_embalse]
    
            st.success(f"Mostrando información y mapa del embalse **{selected_embalse}** (cuenca {selected_cuenca})")
    
            # --- Crear GeoDataFrame y reproyectar ---
            gdf = gpd.GeoDataFrame(df_sel, geometry="geometry", crs="EPSG:25830").to_crs("EPSG:4326")
    
            # --- Extraer geometría y centro ---
            geom = gdf.geometry.iloc[0]
            bounds = geom.bounds  # minx, miny, maxx, maxy
            center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]
    
            # --- Crear mapa centrado ---
            m = folium.Map(location=[center[0], center[1]], zoom_start=11, tiles="CartoDB positron")
    
            folium.GeoJson(
                data=geom.__geo_interface__,
                name=selected_embalse,
                tooltip=folium.Tooltip(selected_embalse),
                style_function=lambda x: {
                    "fillColor": "#2b8cbe",
                    "color": "#045a8d",
                    "weight": 2,
                    "fillOpacity": 0.6,
                },
            ).add_to(m)
    
            folium.LayerControl(position="topright", collapsed=False).add_to(m)
            folium_static(m, width=1000, height=600)
    
            # --- Mostrar información del embalse ---
            st.markdown("### 📋 Información del embalse seleccionado")
            exclude_cols = ["geometry", "report_url"]
            cols = [c for c in gdf.columns if c not in exclude_cols]
    
            # Mostrar la info en formato limpio
            info_df = gdf[cols].T.reset_index()
            info_df.columns = ["Campo", "Valor"]
            df_display = info_df.rename(columns=COLUMN_LABELS)
            st.dataframe(df_display, hide_index=True, use_container_width=True)

    
        else:
            st.info("Selecciona un embalse para visualizarlo en el mapa y ver su información.")
    elif table == "rivers_spain":
        st.markdown("### 🌊 Mapa interactivo de ríos de España")

        import geopandas as gpd
        from shapely import wkb
        from streamlit_folium import folium_static
        import folium
        from sqlalchemy import text
    
        # --- Leer todos los ríos desde la BD convirtiendo la geometría a WKB binario ---
        with engine.connect() as con:
            df_rivers = pd.read_sql(
                text("""
                    SELECT 
                        river_id,
                        river_name,
                        length,
                        ST_AsBinary(geometry) AS geometry
                    FROM rivers_spain
                """),
                con
            )
    
        # Mostrar recuento total de ríos (de forma discreta)
        total_rivers = len(df_rivers)
        st.caption(f"💧 Total de ríos registrados: **{total_rivers}**")
    
        # --- Convertir geometrías WKB a objetos shapely ---
        def safe_load_wkb(geom):
            try:
                if geom is None:
                    return None
                if isinstance(geom, (bytes, bytearray, memoryview)):
                    return wkb.loads(bytes(geom))
            except Exception as e:
                st.write(f"Error convirtiendo geometría: {e}")
                return None
            return None
    
        df_rivers["geometry"] = df_rivers["geometry"].apply(safe_load_wkb)
        df_rivers = df_rivers[df_rivers["geometry"].notnull()]
    
        if df_rivers.empty:
            st.warning("⚠️ No se pudo leer ninguna geometría válida (revisa que la tabla tenga geometrías no nulas).")
            st.stop()
    
        # --- Ordenar ríos por longitud (mayor a menor) ---
        df_rivers = df_rivers.sort_values(by="length", ascending=False)
    
        # --- Selector de río ---
        river_names = df_rivers["river_name"].dropna().unique().tolist()
        selected_river = st.selectbox(
            "🏞️ Selecciona un río:",
            river_names,
            index=None,
            placeholder="Escribe o selecciona un río..."
        )
    
        if selected_river:
            df_sel = df_rivers[df_rivers["river_name"] == selected_river]
    
            river_length = df_sel["length"].iloc[0]
            st.success(f"Mostrando trazado del río **{selected_river}** — Longitud: **{river_length:,.0f} m**")
    
            # Crear GeoDataFrame y reproyectar a WGS84
            gdf = gpd.GeoDataFrame(df_sel, geometry="geometry", crs="EPSG:25830").to_crs("EPSG:4326")
    
            geom = gdf.geometry.iloc[0]
            bounds = geom.bounds
            center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]
    
            # --- Mapa ---
            m = folium.Map(location=[center[0], center[1]], zoom_start=8, tiles="CartoDB positron")
            folium.GeoJson(
                data=geom.__geo_interface__,
                name=selected_river,
                tooltip=folium.Tooltip(f"{selected_river} — {river_length:,.0f} m"),
                style_function=lambda x: {"color": "#1e88e5", "weight": 3},
            ).add_to(m)
    
            folium.LayerControl(position="topright", collapsed=False).add_to(m)
            folium_static(m, width=1000, height=600)
    
            # --- Info del río ---
            st.markdown("### 📋 Información del río seleccionado")
            info_df = gdf[["river_id", "river_name", "length"]].T.reset_index()
            info_df.columns = ["Campo", "Valor"]
            df_display = info_df.rename(columns=COLUMN_LABELS)
            st.dataframe(df_display, hide_index=True, use_container_width=True)

    
        else:
            st.info("Selecciona un río para visualizarlo en el mapa y ver su información.")


# =====================
# Paginación final (dinámica según tipo de tabla)
# =====================

grouped_tables = [
    "samples",
    "profiles_data",
    "insitu_determinations",
    "insitu_sampling",
    "sediment_data",
    "sensor_data"
]

# --- Cálculo de número total de grupos (según tipo de tabla) ---
if table in grouped_tables:
    if not df.empty:
        if table == "insitu_sampling":
            total_groups = len(df.groupby(["extraction_point_id", "sample_date"]))
        elif table == "sediment_data":
            total_groups = len(df.groupby(["extraction_point_id", "sampling_date"]))
        elif table == "insitu_determinations":
            total_groups = len(df.groupby(["extraction_point_id", "date_sampling", "time_sampling"]))
        elif table == "sensor_data":
            total_groups = len(df.groupby(["reservoir_name", "sensor_type"]))
        else:
            time_col = next(
                (c for c in ["date", "datetime", "created_at", "timestamp"]
                 if c in [col["name"] for col in get_cached_columns(engine, table)]),
                None
            )
            if time_col:
                df["hour_group"] = pd.to_datetime(df[time_col]).dt.floor("H")
                total_groups = len(df.groupby(["extraction_point_id", "hour_group"]))
            else:
                total_groups = 0
    else:
        total_groups = 0

    # --- Paginación dinámica ---
    total_pages = max(1, (total_groups + page_size - 1) // page_size)
    start_rec = (page - 1) * page_size + 1 if total_groups > 0 else 0
    end_rec = min(page * page_size, total_groups)

else:
    # --- Modo normal: paginación por filas ---
    total_pages = max(1, (total + page_size - 1) // page_size)
    start_rec = offset + 1 if total > 0 else 0
    end_rec = min(offset + page_size, total)

# =====================
# Controles de navegación (comunes, excepto para mapas)
# =====================

if table not in ["rivers_spain", "reservoirs_spain"]:
    col1, col2, col3 = st.columns([1, 5, 1])

    with col1:
        if page > 1:
            if st.button("⬅️ Anterior"):
                st.session_state["page"] = page - 1
                st.rerun()

    with col2:
        st.markdown(
            f"""
            <div style='text-align:center; font-size:15px;'>
                Página <b>{page}</b> de <b>{total_pages}</b> · 
                Registros {start_rec}–{end_rec}
            </div>
            """,
            unsafe_allow_html=True
        )

        st.markdown(
            """
            <div style='height:6px;'></div>
            <div style='display:flex; justify-content:center; align-items:center; gap:8px;'>
                <span style='font-size:14px; color:#555;'>Ir a página:</span>
            </div>
            """,
            unsafe_allow_html=True
        )

        center_col = st.columns([4, 1, 4])[1]
        with center_col:
            new_page = st.number_input(
                "",
                min_value=1,
                max_value=total_pages,
                value=page,
                step=1,
                label_visibility="collapsed",
                key=f"go_to_page_{table}",
                format="%d"
            )
            if new_page != page:
                st.session_state["page"] = new_page
                st.rerun()

            st.markdown(
                """
                <style>
                div[data-baseweb="input"] > div {
                    width: 70px !important;
                    text-align: center !important;
                    margin: 0 auto !important;
                }
                </style>
                """,
                unsafe_allow_html=True
            )

    with col3:
        if page < total_pages:
            if st.button("Siguiente ➡️"):
                st.session_state["page"] = page + 1
                st.rerun()
else:
    # 🔒 Desactivar paginación en vistas de ríos y embalses
    page = 1

