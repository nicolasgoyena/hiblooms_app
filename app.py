# encoding: utf-8

import ee
import streamlit as st
from datetime import datetime
# ==== i18n: idioma ES/EN con URL ?lang= y session_state ====
from i18n import LANGS as _LANGS, STR as _STR

def _ensure_lang(x: str) -> str:
    return x if x in _LANGS else "es"

def i18n_init():
    qp = st.query_params
    url_lang = qp.get("lang", None)
    if "lang" not in st.session_state:
        st.session_state["lang"] = _ensure_lang(url_lang) if url_lang else "es"
    elif url_lang and url_lang != st.session_state["lang"]:
        st.session_state["lang"] = _ensure_lang(url_lang)

def set_lang(lang: str):
    st.session_state["lang"] = _ensure_lang(lang)
    st.query_params.update({"lang": st.session_state["lang"]})
    st.rerun()

def lang() -> str:
    return _ensure_lang(st.session_state.get("lang", "es"))

def t(key: str) -> str:
    return _STR.get(lang(), {}).get(key) or _STR["es"].get(key, key)

# Inicializar idioma ANTES de set_page_config
i18n_init()

# Usa el título traducido
st.set_page_config(initial_sidebar_state="collapsed", page_title=t("meta.title"), layout="wide")

# Sustituye tu bloque CSS actual por este:
st.markdown(f"""
    <style>
        /* Solo ocultar la navegación mientras NO esté logueado */
        [data-testid="stSidebarNav"] {{
            display: {'none' if not st.session_state.get('logged_in', False) else 'block'} !important;
        }}

        /* Ocultar completamente los enlaces automáticos de encabezados */
        h1 a, h2 a, h3 a {{
            display: none !important;
            pointer-events: none !important;
            text-decoration: none !important;
        }}
    </style>
""", unsafe_allow_html=True)


# Bloquear acceso si no está logueado
if not st.session_state.get("logged_in", False):
    st.switch_page("pages/login.py")
# Inicialización segura del estado
default_keys = {
    "cloud_results": [],
    "used_cloud_results": [],
    "data_time": [],
    "urls_exportacion": [],
    "available_dates": [],
    "selected_indices": [],
}

for key, default in default_keys.items():
    if key not in st.session_state:
        st.session_state[key] = default
# Inicialización de las variables 'image_list' y 'selected_dates'
if "image_list" not in st.session_state:
    st.session_state["image_list"] = []

if "selected_dates" not in st.session_state:
    st.session_state["selected_dates"] = []
import geemap.foliumap as geemap
from streamlit_folium import folium_static
from datetime import datetime
import pandas as pd
import altair as alt
from dateutil.relativedelta import relativedelta  # Para calcular ±3 meses
import folium
import geopandas as gpd
import os
import time
from datetime import timedelta
import json
import numpy as np
import requests as _requests
from hiblooms_core import (
    get_available_dates,
    process_sentinel2,
    get_values_at_point,
    calcular_media_diaria_embalse,
    generar_url_geotiff_multibanda,
)
from hiblooms_calibration import render_calibration_tab
try:
    from streamlit_autorefresh import st_autorefresh as _st_autorefresh
    _AUTOREFRESH_AVAILABLE = True
except ImportError:
    _AUTOREFRESH_AVAILABLE = False

# URL de la API de jobs asíncrona (configurar en .streamlit/secrets.toml como api_url)
_API_URL = st.secrets.get("api_url", "http://localhost:8000")

try:
    if "GEE_SERVICE_ACCOUNT_JSON" in st.secrets:

        # Convertir el JSON guardado en Streamlit Secrets a un diccionario
        json_object = json.loads(st.secrets["GEE_SERVICE_ACCOUNT_JSON"], strict=False)
        service_account = json_object["client_email"]
        json_object = json.dumps(json_object)

        # Autenticar con la cuenta de servicio
        credentials = ee.ServiceAccountCredentials(service_account, key_data=json_object)
        ee.Initialize(credentials)

    else:
        st.write("🔍 Intentando inicializar GEE localmente...")
        ee.Initialize()

except Exception as e:
    st.error(f"❌ No se pudo inicializar Google Earth Engine: {str(e)}")
    st.stop()

# URL pública del archivo CSV en S3
url_csv = "puntos_interes.csv"

try:
    df_poi = pd.read_csv(url_csv)
    puntos_interes = {}
    def _reservoir_key(name: str) -> str | None:
        if not name:
            return None
        n = name.strip().lower()
        if n in ("el val", "val"):
            return "val"
        if n in ("bellús", "bellus"):
            return "bellus"
        return None
    
    def get_available_indices_for_reservoir(reservoir_name: str) -> list[str]:
        base = ["MCI", "PCI_B5/B4", "NDCI_ind", "UV_PC_Gral_cal"]  # índices generales
        rk = _reservoir_key(reservoir_name)
        if rk == "val":
            base += ["PC_Val_cal", "Chla_Val_cal"]
        elif rk == "bellus":
            base += ["Chla_Bellus_cal", "PC_Bellus_cal"]
        return base

    for _, row in df_poi.iterrows():
        embalse = row["embalse"]
        if embalse not in puntos_interes:
            puntos_interes[embalse] = {}
        puntos_interes[embalse][row["nombre"]] = (row["latitud"], row["longitud"])
except Exception as e:
    st.error(f"Error cargando puntos de interés desde S3: {e}")
    puntos_interes = {}


@st.cache_data
def cargar_csv_desde_url(url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(url)

        if 'Time' in df.columns:
            df.rename(columns={'Time': 'Fecha-hora'}, inplace=True)

        df['Fecha-hora'] = pd.to_datetime(df['Fecha-hora'], format='mixed')

        return df
    except Exception as e:
        st.warning(f"⚠️ Error al cargar el CSV desde {url}: {e}")
        return pd.DataFrame()


def obtener_nombres_embalses(shapefile_path="shapefiles/embalses_hiblooms.shp"):
    if os.path.exists(shapefile_path):
        gdf = gpd.read_file(shapefile_path)

        if "NOMBRE" in gdf.columns:
            nombres_embalses = sorted(gdf["NOMBRE"].dropna().unique())
            return nombres_embalses
        else:
            st.error("❌ El shapefile cargado no contiene una columna llamada 'NOMBRE'. No se pueden mostrar embalses.")
            return []
    else:
        st.error(f"No se encontró el archivo {shapefile_path}.")
        return []

# Función combinada para cargar el shapefile, ajustar el zoom y mostrar los embalses con tooltip
def cargar_y_mostrar_embalses(map_object, shapefile_path="shapefiles/embalses_hiblooms.shp", nombre_columna="NOMBRE"):
    if os.path.exists(shapefile_path):
        gdf_embalses = gpd.read_file(shapefile_path).to_crs(epsg=4326)  # Convertir a WGS84

        # Ajustar el zoom automáticamente a la extensión de los embalses
        bounds = gdf_embalses.total_bounds  # (minx, miny, maxx, maxy)
        map_object.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

        for _, row in gdf_embalses.iterrows():
            nombre_embalse = row.get(nombre_columna, "Embalse desconocido")  # Obtener el nombre real

            if row.geometry.geom_type == 'Point':
                folium.Marker(                    location=[row.geometry.y, row.geometry.x],
                    popup=nombre_embalse,
                    tooltip=nombre_embalse,  # Muestra el nombre al hacer hover
                    icon=folium.Icon(color="blue", icon="tint")
                ).add_to(map_object)

            elif row.geometry.geom_type in ['Polygon', 'MultiPolygon']:
                folium.GeoJson(
                    row.geometry,
                    name=nombre_embalse,
                    tooltip=folium.Tooltip(nombre_embalse),  # Muestra el nombre al hacer hover
                    style_function=lambda x: {"fillColor": "blue", "color": "blue", "weight": 2, "fillOpacity": 0.4}
                ).add_to(map_object)

    else:
        st.error(f"No se encontró el archivo {shapefile_path}.")

def load_reservoir_shapefile(reservoir_name, shapefile_path="shapefiles/embalses_hiblooms.shp"):
    if os.path.exists(shapefile_path):
        gdf = gpd.read_file(shapefile_path)

        # Verificar existencia del campo 'NOMBRE'
        if "NOMBRE" not in gdf.columns:
            st.error("❌ El shapefile cargado no contiene una columna llamada 'NOMBRE'. Añádela para poder seleccionar embalses.")
            return None

        # Reproyectar automáticamente a EPSG:4326 si no lo está
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        # Normalizar nombres
        gdf["NOMBRE"] = gdf["NOMBRE"].str.lower().str.replace(" ", "_")
        normalized_name = reservoir_name.lower().replace(" ", "_")

        gdf_filtered = gdf[gdf["NOMBRE"] == normalized_name]

        if gdf_filtered.empty:
            st.error(f"No se encontró el embalse {reservoir_name} en el shapefile.")
            return None

        return gdf_filtered
    else:
        st.error(f"No se encontró el archivo {shapefile_path}.")
        return None

def gdf_to_ee_geometry(gdf):
    if gdf.empty:
        raise ValueError("❌ El shapefile está vacío o no contiene geometrías.")
    
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        raise ValueError("❌ El GeoDataFrame debe estar en EPSG:4326.")

    geometry = gdf.geometry.iloc[0]

    if geometry.geom_type == "MultiPolygon":
        geometry = list(geometry.geoms)[0]  # Extrae el primer polígono

    ee_coordinates = list(geometry.exterior.coords)

    ee_geometry = ee.Geometry.Polygon(
        [ee_coordinates],
        geodesic=False  # Suele ser preferible para polígonos pequeños
    )

    return ee_geometry


def generar_leyenda(indices_seleccionados):
    # Parámetros de visualización para cada índice
    parametros = {
        "MCI": {"min": -0.1, "max": 0.4, "palette": ['blue', 'green', 'yellow', 'red']},
        "PCI_B5/B4": {"min": 0.5, "max": 1.5, "palette": ["#ADD8E6", "#008000", "#FFFF00", "#FF0000"]},
        "NDCI_ind": {"min": -0.1, "max": 0.4, "palette": ['blue', 'green', 'yellow', 'red']},
        "PC_Val_cal": {"min": 0, "max": 5, "palette": ["#ADD8E6", "#008000", "#FFFF00", "#FF0000"]},
        "Chla_Val_cal": {"min": 0,"max": 150,"palette": ['#2171b5', '#75ba82', '#fdae61', '#e31a1c']},
        "Chla_Bellus_cal": {"min": 5,"max": 100,"palette": ['#2171b5', '#75ba82', '#fdae61', '#e31a1c']},
        "PC_Bellus_cal": {"min": 25,"max": 500,"palette": ['#2171b5', '#75ba82', '#fdae61', '#e31a1c']},
        "UV_PC_Gral_cal": {"min": 0,"max": 100,"palette": ['#2171b5', '#75ba82', '#fdae61', '#e31a1c']}
    }

    leyenda_html = "<div style='border: 2px solid #ddd; padding: 10px; border-radius: 5px; background-color: white;'>"
    leyenda_html += f"<h4 style='text-align: center;'>{t('legend.title')}</h4>"

    # Leyenda para la capa SCL (Scene Classification Layer)
    scl_palette = {
        1: ('#ff0004', t('scl.1')),
        2: ('#000000', t('scl.2')),
        3: ('#8B4513', t('scl.3')),
        4: ('#00FF00', t('scl.4')),
        5: ('#FFD700', t('scl.5')),
        6: ('#0000FF', t('scl.6')),
        7: ('#F4EEEC', t('scl.7')),
        8: ('#C8C2C0', t('scl.8')),
        9: ('#706C6B', t('scl.9')),
        10: ('#87CEFA', t('scl.10')),
        11: ('#00FFFF', t('scl.11'))
    }

    leyenda_html += "<b>SCL:</b><br>"
    for _, (color, desc) in scl_palette.items():
        leyenda_html += f"<div style='display: flex; align-items: center;'><div style='width: 15px; height: 15px; background-color: {color}; border: 1px solid black; margin-right: 5px;'></div> {desc}</div>"

    leyenda_html += "<br>"

    # Leyenda para la capa MSK_CLDPRB (Probabilidad de nubes)
    msk_palette = ["blue", "green", "yellow", "red", "black"]
    leyenda_html += "<b>Capa MSK_CLDPRB (Probabilidad de Nubes):</b><br>"
    leyenda_html += f"<div style='background: linear-gradient(to right, {', '.join(msk_palette)}); height: 20px; border: 1px solid #000;'></div>"
    leyenda_html += "<div style='display: flex; justify-content: space-between; font-size: 12px;'><span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span></div>"
    leyenda_html += "<br>"

    # Leyenda para los índices seleccionados
    for indice in indices_seleccionados:
        if indice in parametros:
            min_val = parametros[indice]["min"]
            max_val = parametros[indice]["max"]
            palette = parametros[indice]["palette"]

            # Construcción del gradiente CSS
            gradient_colors = ", ".join(palette)
            gradient_style = f"background: linear-gradient(to right, {gradient_colors}); height: 20px; border: 1px solid #000;"

            leyenda_html += f"<b>{indice}:</b><br>"
            leyenda_html += f"<div style='{gradient_style}'></div>"

            # Crear marcadores intermedios
            markers_html = "<div style='display: flex; justify-content: space-between; margin-top: 5px;'>"
            num_colores = len(palette)
            for i in range(num_colores):
                valor = min_val + (max_val - min_val) * i / (num_colores - 1) if num_colores > 1 else min_val
                valor_formateado = f"{valor:.2f}" if isinstance(valor, float) else str(valor)

                markers_html += (
                    "<div style='display: flex; flex-direction: column; align-items: center;'>"
                    "<div style='width: 1px; height: 8px; background-color: black;'></div>"
                    f"<span style='font-size: 12px;'>{valor_formateado}</span>"
                    "</div>"
                )
            markers_html += "</div>"
            leyenda_html += markers_html + "<br>"

    leyenda_html += "</div>"

    # Mostrar la leyenda en Streamlit
    st.markdown(leyenda_html, unsafe_allow_html=True)

# INTERFAZ DE STREAMLIT

# ── Cargar CSS tema oscuro ─────────────────────────────────────
with open("styles.css", "r", encoding="utf-8") as _f:
    st.markdown(f"<style>{_f.read()}</style>", unsafe_allow_html=True)

# ── Header: logos originales + título + idioma ────────────────
_hdr_left, _hdr_center, _hdr_right = st.columns([1.6, 3.0, 1.8])

with _hdr_left:
    st.image("images/logo_hiblooms.png", width=240)
    _min_c1, _min_c2 = st.columns([1.6, 1])
    with _min_c1:
        st.image("images/ministerio.png", width=160)
    with _min_c2:
        st.image("images/logo_unav.png", width=90)

with _hdr_center:
    st.markdown(
        f"""
        <div class="hb-hero">
          <div class="hb-hero-title">
            HI<span>BLOOMS</span>
          </div>
          <div class="hb-hero-sub">
            🛰&nbsp; {t("hero.l1")} &nbsp;·&nbsp; {t("hero.l2")}
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

with _hdr_right:
    st.image("images/logo_bioma.png", width=200)
    _ebro_c, _jucar_c = st.columns([1, 1.2])
    with _ebro_c:
        st.image("images/logo_ebro.png", width=100)
    with _jucar_c:
        st.image("images/logo_jucar.png", width=120)
    chosen = st.selectbox(
        t("ui.language"),
        options=_LANGS,
        format_func=lambda x: "🇪🇸 Español" if x == "es" else "🇬🇧 English",
        index=_LANGS.index(lang()),
    )
    if chosen != lang():
        set_lang(chosen)

st.markdown('<div class="hb-divider"></div>', unsafe_allow_html=True)



tab1, tab2, tab3, tab4, tab5 = st.tabs([t("tabs.intro"), t("tabs.calibration"), t("tabs.map"), t("tabs.tables"), t("tabs.quick")])
with tab1:

    # ── HERO BANNER ────────────────────────────────────────────
    _hero_es = """
        <div style="position:relative;border-radius:16px;overflow:hidden;background:linear-gradient(135deg,#0a2a2a 0%,#0d3d35 40%,#0a2e3a 100%);padding:2.8rem 3rem;margin-bottom:1.5rem;box-shadow:0 4px 24px rgba(0,168,150,.18)">
            <svg style="position:absolute;inset:0;width:100%;height:100%;opacity:.08" viewBox="0 0 800 200" preserveAspectRatio="xMidYMid slice">
                <defs>
                    <pattern id="wavep" x="0" y="0" width="120" height="40" patternUnits="userSpaceOnUse"><path d="M0 20 Q30 5 60 20 Q90 35 120 20" stroke="#00e5b4" stroke-width="1.5" fill="none"/></pattern>
                    <pattern id="dotsp" x="0" y="0" width="30" height="30" patternUnits="userSpaceOnUse"><circle cx="15" cy="15" r="1.2" fill="#00d4ff"/></pattern>
                </defs>
                <rect width="100%" height="100%" fill="url(#dotsp)"/>
                <rect width="100%" height="100%" fill="url(#wavep)" opacity="0.6"/>
            </svg>
            <svg style="position:absolute;right:2rem;top:50%;transform:translateY(-50%);opacity:.12;width:160px;height:160px" viewBox="0 0 160 160">
                <ellipse cx="80" cy="80" rx="70" ry="30" stroke="#00d4ff" stroke-width="1" fill="none" stroke-dasharray="4 3"/>
                <ellipse cx="80" cy="80" rx="50" ry="50" stroke="#00e5b4" stroke-width="0.8" fill="none" stroke-dasharray="3 4"/>
                <circle cx="80" cy="50" r="5" fill="#00d4ff"/>
                <rect x="75" y="46" width="10" height="8" fill="none" stroke="#00d4ff" stroke-width="0.8"/>
                <rect x="64" y="49" width="10" height="3" fill="#00d4ff" opacity=".6"/>
                <rect x="86" y="49" width="10" height="3" fill="#00d4ff" opacity=".6"/>
            </svg>
            <div style="position:relative;z-index:1;max-width:75%">
                <div style="display:inline-flex;align-items:center;gap:6px;background:rgba(0,212,255,.12);border:1px solid rgba(0,212,255,.25);border-radius:999px;padding:3px 12px;margin-bottom:1rem">
                    <span style="width:6px;height:6px;border-radius:50%;background:#39d98a;display:inline-block"></span>
                    <span style="font-size:11px;font-weight:600;letter-spacing:.08em;color:#00d4ff;font-family:DM Sans,sans-serif;text-transform:uppercase">Monitorización activa · Sentinel-2</span>
                </div>
                <div style="font-family:Cabinet Grotesk,sans-serif;font-size:clamp(1.4rem,2.5vw,2rem);font-weight:900;color:#fff;line-height:1.15;letter-spacing:-.02em;margin-bottom:.75rem">
                    Vigilancia satelital de<br><span style="color:#00e5b4">cianobacterias</span> en embalses
                </div>
                <div style="font-size:13px;color:rgba(255,255,255,.6);font-family:DM Sans,sans-serif;line-height:1.6;max-width:520px">
                    Reconstrucción histórica y monitorización en tiempo casi-real de la proliferación de cianobacterias en embalses españoles mediante teledetección Sentinel-2 · <span style="color:#00e5b4;font-weight:500">PID2023-153234OB-I00</span>
                </div>
                <div style="display:flex;gap:10px;margin-top:1.25rem;flex-wrap:wrap">
                    <div style="background:rgba(0,229,180,.12);border:1px solid rgba(0,229,180,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#00e5b4;font-family:DM Sans,sans-serif;font-weight:500">🛰 24 embalses</div>
                    <div style="background:rgba(0,212,255,.12);border:1px solid rgba(0,212,255,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#00d4ff;font-family:DM Sans,sans-serif;font-weight:500">📅 Desde 2017</div>
                    <div style="background:rgba(240,165,0,.12);border:1px solid rgba(240,165,0,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#f0a500;font-family:DM Sans,sans-serif;font-weight:500">⚠ 3 alertas activas</div>
                </div>
            </div>
        </div>
    """

    _hero_en = """
        <div style="position:relative;border-radius:16px;overflow:hidden;background:linear-gradient(135deg,#0a2a2a 0%,#0d3d35 40%,#0a2e3a 100%);padding:2.8rem 3rem;margin-bottom:1.5rem;box-shadow:0 4px 24px rgba(0,168,150,.18)">
            <svg style="position:absolute;inset:0;width:100%;height:100%;opacity:.08" viewBox="0 0 800 200" preserveAspectRatio="xMidYMid slice">
                <defs>
                    <pattern id="wavep2" x="0" y="0" width="120" height="40" patternUnits="userSpaceOnUse"><path d="M0 20 Q30 5 60 20 Q90 35 120 20" stroke="#00e5b4" stroke-width="1.5" fill="none"/></pattern>
                    <pattern id="dotsp2" x="0" y="0" width="30" height="30" patternUnits="userSpaceOnUse"><circle cx="15" cy="15" r="1.2" fill="#00d4ff"/></pattern>
                </defs>
                <rect width="100%" height="100%" fill="url(#dotsp2)"/>
                <rect width="100%" height="100%" fill="url(#wavep2)" opacity="0.6"/>
            </svg>
            <svg style="position:absolute;right:2rem;top:50%;transform:translateY(-50%);opacity:.12;width:160px;height:160px" viewBox="0 0 160 160">
                <ellipse cx="80" cy="80" rx="70" ry="30" stroke="#00d4ff" stroke-width="1" fill="none" stroke-dasharray="4 3"/>
                <ellipse cx="80" cy="80" rx="50" ry="50" stroke="#00e5b4" stroke-width="0.8" fill="none" stroke-dasharray="3 4"/>
                <circle cx="80" cy="50" r="5" fill="#00d4ff"/>
                <rect x="75" y="46" width="10" height="8" fill="none" stroke="#00d4ff" stroke-width="0.8"/>
                <rect x="64" y="49" width="10" height="3" fill="#00d4ff" opacity=".6"/>
                <rect x="86" y="49" width="10" height="3" fill="#00d4ff" opacity=".6"/>
            </svg>
            <div style="position:relative;z-index:1;max-width:75%">
                <div style="display:inline-flex;align-items:center;gap:6px;background:rgba(0,212,255,.12);border:1px solid rgba(0,212,255,.25);border-radius:999px;padding:3px 12px;margin-bottom:1rem">
                    <span style="width:6px;height:6px;border-radius:50%;background:#39d98a;display:inline-block"></span>
                    <span style="font-size:11px;font-weight:600;letter-spacing:.08em;color:#00d4ff;font-family:DM Sans,sans-serif;text-transform:uppercase">Active monitoring · Sentinel-2</span>
                </div>
                <div style="font-family:Cabinet Grotesk,sans-serif;font-size:clamp(1.4rem,2.5vw,2rem);font-weight:900;color:#fff;line-height:1.15;letter-spacing:-.02em;margin-bottom:.75rem">
                    Satellite surveillance of<br><span style="color:#00e5b4">cyanobacteria</span> in reservoirs
                </div>
                <div style="font-size:13px;color:rgba(255,255,255,.6);font-family:DM Sans,sans-serif;line-height:1.6;max-width:520px">
                    Historical reconstruction and near real-time monitoring of cyanobacterial blooms in Spanish reservoirs using Sentinel-2 remote sensing · <span style="color:#00e5b4;font-weight:500">PID2023-153234OB-I00</span>
                </div>
                <div style="display:flex;gap:10px;margin-top:1.25rem;flex-wrap:wrap">
                    <div style="background:rgba(0,229,180,.12);border:1px solid rgba(0,229,180,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#00e5b4;font-family:DM Sans,sans-serif;font-weight:500">🛰 24 reservoirs</div>
                    <div style="background:rgba(0,212,255,.12);border:1px solid rgba(0,212,255,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#00d4ff;font-family:DM Sans,sans-serif;font-weight:500">📅 Since 2017</div>
                    <div style="background:rgba(240,165,0,.12);border:1px solid rgba(240,165,0,.25);border-radius:8px;padding:6px 14px;font-size:12px;color:#f0a500;font-family:DM Sans,sans-serif;font-weight:500">⚠ 3 active alerts</div>
                </div>
            </div>
        </div>
    """

    # ── DIAGRAMA FLUJO SENTINEL-2 ───────────────────────────────
    _diagram_es = """
        <div style="background:#fff;border:1px solid #e2ecf0;border-radius:16px;padding:1.5rem 2rem;margin-bottom:1.5rem;box-shadow:0 1px 3px rgba(15,31,46,.06)">
            <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#8fa3b0;margin-bottom:1.25rem;display:flex;align-items:center;gap:8px">
                <span>Flujo de datos · de satélite a alerta</span>
                <span style="flex:1;height:1px;background:#e2ecf0;display:block"></span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;gap:0;align-items:center">
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🛰</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Sentinel-2</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Imagen multiespectral<br>cada 5 días</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">→</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">☁</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Google Earth Engine</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Filtrado de nubes<br>y preprocesado L2A</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">→</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">📐</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Índices espectrales</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">MCI · NDCI · PCI<br>Chl-a · Ficocianina</div>
                </div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;margin-top:4px">
                <div></div><div></div>
                <div style="text-align:center;color:#00a896;font-size:20px">↓</div>
                <div></div><div></div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;gap:0;align-items:center;margin-top:4px">
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#fef3c7;border:2px solid #f59e0b;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🚨</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Alerta de bloom</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Nivel de riesgo<br>y notificación</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">←</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">📊</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Calibración in situ</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Ajuste con datos<br>de campo medidos</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">←</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🗺</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Mapas de concentración</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Clorofila-a y<br>ficocianina (µg/L)</div>
                </div>
            </div>
        </div>
    """

    _diagram_en = """
        <div style="background:#fff;border:1px solid #e2ecf0;border-radius:16px;padding:1.5rem 2rem;margin-bottom:1.5rem;box-shadow:0 1px 3px rgba(15,31,46,.06)">
            <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#8fa3b0;margin-bottom:1.25rem;display:flex;align-items:center;gap:8px">
                <span>Data flow · from satellite to alert</span>
                <span style="flex:1;height:1px;background:#e2ecf0;display:block"></span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;gap:0;align-items:center">
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🛰</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Sentinel-2</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Multispectral image<br>every 5 days</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">→</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">☁</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Google Earth Engine</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Cloud filtering<br>& L2A preprocessing</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">→</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">📐</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Spectral indices</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">MCI · NDCI · PCI<br>Chl-a · Phycocyanin</div>
                </div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;margin-top:4px">
                <div></div><div></div>
                <div style="text-align:center;color:#00a896;font-size:20px">↓</div>
                <div></div><div></div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 28px 1fr 28px 1fr;gap:0;align-items:center;margin-top:4px">
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#fef3c7;border:2px solid #f59e0b;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🚨</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Bloom alert</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Risk level<br>& notification</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">←</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">📊</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">In-situ calibration</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Tuned with field<br>measurements</div>
                </div>
                <div style="text-align:center;color:#00a896;font-size:20px;font-weight:300;padding-bottom:18px">←</div>
                <div style="text-align:center;padding:.5rem">
                    <div style="width:54px;height:54px;border-radius:50%;background:#e6f7f5;border:2px solid #00a896;display:flex;align-items:center;justify-content:center;margin:0 auto .6rem;font-size:24px">🗺</div>
                    <div style="font-size:12px;font-weight:700;color:#0f1f2e;margin-bottom:.2rem;font-family:DM Sans,sans-serif">Concentration maps</div>
                    <div style="font-size:11px;color:#8fa3b0;font-family:DM Sans,sans-serif;line-height:1.4">Chl-a &<br>phycocyanin (µg/L)</div>
                </div>
            </div>
        </div>
    """

    # ==== Contenido bilingüe ====
    if lang() == "es":
        st.markdown(_hero_es, unsafe_allow_html=True)
        st.markdown(_diagram_es, unsafe_allow_html=True)
    # ==== Contenido bilingüe ====
    if lang() == "es":
        st.markdown("""
            <div class="hb-info-panel" style="text-align:center; border-left-width:0; border-top: 3px solid #00a896; background: linear-gradient(135deg, rgba(0,168,150,0.08), rgba(16,185,129,0.05));">
              <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#4a6372;margin-bottom:.5rem;">PID2023-153234OB-I00</div>
              <div style="font-size:1.05rem;font-weight:700;color:#0f1f2e;line-height:1.4;">Reconstrucción histórica y estado actual de la proliferación de cianobacterias en embalses españoles: <span style="color:#00a896;">HIBLOOMS</span></div>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("""
            <div class="hb-info-panel">
              <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#4a6372;margin-bottom:.5rem;">Alineación con estrategias nacionales</div>
              <div style="margin-top:.5rem;display:flex;flex-direction:column;gap:.4rem;">
                <div><span class="hb-badge hb-badge-info">PNACC</span> Plan Nacional de Adaptación al Cambio Climático (2021-2030)</div>
                <div><span class="hb-badge hb-badge-info">DMA</span> Directiva Marco del Agua 2000/60/EC</div>
                <div><span class="hb-badge hb-badge-ok">ODS 6</span> Agua limpia y saneamiento</div>
              </div>
            </div>
        """, unsafe_allow_html=True)

        st.markdown(
            '<div class="hb-info-panel"><b>Alineación con estrategias nacionales:</b><br>📌 Plan Nacional de Adaptación al Cambio Climático (PNACC 2021-2030)<br>📌 Directiva Marco del Agua 2000/60/EC<br>📌 Objetivo de Desarrollo Sostenible 6: Agua limpia y saneamiento</div>',
            unsafe_allow_html=True)

        st.subheader("Justificación")
        st.markdown("""
            La proliferación de cianobacterias en embalses es una preocupación ambiental y de salud pública.
            El proyecto **HIBLOOMS** busca evaluar la evolución histórica y actual de estos eventos en los embalses de España, contribuyendo a:
            - La monitorización de parámetros clave del cambio climático y sus efectos en los ecosistemas acuáticos.
            - La identificación de factores ambientales y de contaminación que influyen en la proliferación de cianobacterias.
            - La generación de información para mejorar la gestión y calidad del agua en España.
        """)

        st.subheader("Hipótesis y Relevancia del Proyecto")
        st.markdown("""
            Se estima que **40% de los embalses españoles** son susceptibles a episodios de proliferación de cianobacterias.
            En un contexto de cambio climático, donde las temperaturas y la eutrofización aumentan, el riesgo de proliferaciones tóxicas es mayor.

            🛰 **¿Cómo abordamos este desafío?**
            - Uso de **teledetección satelital** para monitoreo en tiempo real.
            - Implementación de **técnicas avanzadas de análisis ambiental** para evaluar las causas y patrones de proliferación.
            - Creación de modelos para predecir episodios de blooms y sus impactos en la salud y el medio ambiente.
        """)

        st.subheader("Impacto esperado")
        st.markdown("""
            El proyecto contribuirá significativamente a la gestión sostenible de embalses, proporcionando herramientas innovadoras para:
            - Evaluar la **calidad del agua** con técnicas avanzadas.
            - Diseñar estrategias de mitigación para **minimizar el riesgo de toxicidad**.
            - Colaborar con administraciones públicas y expertos para la **toma de decisiones basada en datos**.
        """)

        st.subheader("Equipo de Investigación")
        st.markdown("""
            <div class="hb-info-panel">
                <b>Equipo de Investigación:</b><br>
                🔬 <b>David Elustondo (DEV)</b> - BIOMA/UNAV, calidad del agua, QA/QC y biogeoquímica.<br>
                🔬 <b>Yasser Morera Gómez (YMG)</b> - BIOMA/UNAV, geoquímica isotópica y geocronología con <sup>210</sup>Pb.<br>
                🔬 <b>Esther Lasheras Adot (ELA)</b> - BIOMA/UNAV, técnicas analíticas y calidad del agua.<br>
                🔬 <b>Jesús Miguel Santamaría (JSU)</b> - BIOMA/UNAV, calidad del agua y técnicas analíticas.<br>
                🔬 <b>Carolina Santamaría Elola (CSE)</b> - BIOMA/UNAV, técnicas analíticas y calidad del agua.<br>
                🔬 <b>Adriana Rodríguez Garraus (ARG)</b> - MITOX/UNAV, análisis toxicológico.<br>
                🔬 <b>Sheila Izquieta Rojano (SIR)</b> - BIOMA/UNAV, SIG y teledetección, datos FAIR, digitalización.<br>
            </div>

            <div class="hb-info-panel">
                <b>Equipo de Trabajo:</b><br>
                🔬 <b>Aimee Valle Pombrol (AVP)</b> - BIOMA/UNAV, taxonomía de cianobacterias e identificación de toxinas.<br>
                🔬 <b>Carlos Manuel Alonso Hernández (CAH)</b> - Laboratorio de Radioecología/IAEA, geocronología con <sup>210</sup>Pb.<br>
                🔬 <b>David Widory (DWI)</b> - GEOTOP/UQAM, geoquímica isotópica y calidad del agua.<br>
                🔬 <b>Ángel Ramón Moreira González (AMG)</b> - CEAC, taxonomía de fitoplancton y algas.<br>
                🔬 <b>Augusto Abilio Comas González (ACG)</b> - CEAC, taxonomía de cianobacterias y ecología acuática.<br>
                🔬 <b>Lorea Pérez Babace (LPB)</b> - BIOMA/UNAV, técnicas analíticas y muestreo de campo.<br>
                🔬 <b>José Miguel Otano Calvente (JOC)</b> - BIOMA/UNAV, técnicas analíticas y muestreo de campo.<br>
                🔬 <b>Alain Suescun Santamaría (ASS)</b> - BIOMA/UNAV, técnicas analíticas.<br>
                🔬 <b>Leyre López Alonso (LLA)</b> - BIOMA/UNAV, análisis de datos.<br>
                🔬 <b>María José Rodríguez Pérez (MRP)</b> - Confederación Hidrográfica del Ebro, calidad del agua.<br>
                🔬 <b>María Concepción Durán Lalaguna (MDL)</b> - Confederación Hidrográfica del Júcar, calidad del agua.<br>
            </div>
        """, unsafe_allow_html=True)

        st.success("🔬 HIBLOOMS no solo estudia el presente, sino que reconstruye el pasado para entender el futuro de la calidad del agua en España.")

    else:
        st.markdown(
            '<div class="hb-info-panel" style="text-align:center;border-left-width:0;border-top:3px solid #00a896;background:linear-gradient(135deg,rgba(0,168,150,.08),rgba(16,185,129,.05));"><div style=\"font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#4a6372;margin-bottom:.4rem;\">PID2023-153234OB-I00</div><div style=\"font-size:1.05rem;font-weight:700;color:#0f1f2e;line-height:1.4;\">Historical Reconstruction and Current Status of Cyanobacterial Blooms in Spanish Reservoirs: <span style=\"color:#00a896;\">HIBLOOMS</span></div></div>',
            unsafe_allow_html=True)

        st.markdown(
            '<div class="hb-info-panel"><b>Alignment with National Strategies:</b><br>📌 National Climate Change Adaptation Plan (PNACC 2021–2030)<br>📌 EU Water Framework Directive 2000/60/EC<br>📌 Sustainable Development Goal 6: Clean Water and Sanitation</div>',
            unsafe_allow_html=True)

        st.subheader("Rationale")
        st.markdown("""
            The proliferation of cyanobacteria in reservoirs is an environmental and public health concern.
            The **HIBLOOMS** project aims to assess the historical and current evolution of these events in Spanish reservoirs, contributing to:
            - Monitoring key climate change parameters and their effects on aquatic ecosystems.
            - Identifying environmental and pollution factors influencing cyanobacterial blooms.
            - Generating information to improve water management and water quality in Spain.
        """)

        st.subheader("Hypothesis and Project Relevance")
        st.markdown("""
            It is estimated that **40% of Spanish reservoirs** are susceptible to cyanobacterial bloom episodes.
            In a context of climate change, where temperatures and eutrophication are increasing, the risk of toxic blooms is even higher.

            🛰 **How do we address this challenge?**
            - Use of **satellite remote sensing** for near real-time monitoring.
            - Implementation of **advanced environmental analysis techniques** to assess the causes and patterns of blooms.
            - Development of models to predict bloom episodes and their impacts on health and the environment.
        """)

        st.subheader("Expected Impact")
        st.markdown("""
            The project will significantly contribute to the sustainable management of reservoirs by providing innovative tools to:
            - Assess **water quality** with advanced techniques.
            - Design mitigation strategies to **minimize toxicity risks**.
            - Collaborate with public administrations and experts for **data-driven decision making**.
        """)

        st.subheader("Research Team")
        st.markdown("""
            <div class="hb-info-panel">
                <b>Research Team:</b><br>
                🔬 <b>David Elustondo (DEV)</b> – BIOMA/UNAV, water quality, QA/QC, biogeochemistry.<br>
                🔬 <b>Yasser Morera Gómez (YMG)</b> – BIOMA/UNAV, isotopic geochemistry and <sup>210</sup>Pb geochronology.<br>
                🔬 <b>Esther Lasheras Adot (ELA)</b> – BIOMA/UNAV, analytical techniques, water quality.<br>
                🔬 <b>Jesús Miguel Santamaría (JSU)</b> – BIOMA/UNAV, water quality, analytical techniques.<br>
                🔬 <b>Carolina Santamaría Elola (CSE)</b> – BIOMA/UNAV, analytical techniques, water quality.<br>
                🔬 <b>Adriana Rodríguez Garraus (ARG)</b> – MITOX/UNAV, toxicological analysis.<br>
                🔬 <b>Sheila Izquieta Rojano (SIR)</b> – BIOMA/UNAV, GIS & remote sensing, FAIR data, digitalization.<br>
            </div>

            <div class="hb-info-panel">
                <b>Collaborating Team:</b><br>
                🔬 <b>Aimee Valle Pombrol (AVP)</b> – BIOMA/UNAV, cyanobacteria taxonomy and toxin identification.<br>
                🔬 <b>Carlos Manuel Alonso Hernández (CAH)</b> – Radioecology Laboratory/IAEA, <sup>210</sup>Pb geochronology.<br>
                🔬 <b>David Widory (DWI)</b> – GEOTOP/UQAM, isotopic geochemistry, water quality.<br>
                🔬 <b>Ángel Ramón Moreira González (AMG)</b> – CEAC, phytoplankton and algae taxonomy.<br>
                🔬 <b>Augusto Abilio Comas González (ACG)</b> – CEAC, cyanobacteria taxonomy, aquatic ecology.<br>
                🔬 <b>Lorea Pérez Babace (LPB)</b> – BIOMA/UNAV, analytical techniques, field sampling.<br>
                🔬 <b>José Miguel Otano Calvente (JOC)</b> – BIOMA/UNAV, analytical techniques, field sampling.<br>
                🔬 <b>Alain Suescun Santamaría (ASS)</b> – BIOMA/UNAV, analytical techniques.<br>
                🔬 <b>Leyre López Alonso (LLA)</b> – BIOMA/UNAV, data analysis.<br>
                🔬 <b>María José Rodríguez Pérez (MRP)</b> – Ebro River Basin Authority, water quality.<br>
                🔬 <b>María Concepción Durán Lalaguna (MDL)</b> – Júcar River Basin Authority, water quality.<br>
            </div>
        """, unsafe_allow_html=True)

        st.success("🔬 HIBLOOMS not only studies the present, but also reconstructs the past to better understand the future of water quality in Spain.")

with tab2:
    render_calibration_tab(obtener_nombres_embalses, load_reservoir_shapefile, gdf_to_ee_geometry, lang=lang())

with tab3:
    st.subheader(t("upload.shp.h"))
    st.info(t("upload.shp.i"))
    uploaded_zip = st.file_uploader(t("upload.shp.zip"), type=["zip"])

    custom_shapefile_path = None

    if uploaded_zip is not None:
        import zipfile
        import tempfile

        temp_dir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(uploaded_zip, "r") as zip_ref:
            zip_ref.extractall(temp_dir.name)

        for file in os.listdir(temp_dir.name):
            if file.endswith(".shp"):
                custom_shapefile_path = os.path.join(temp_dir.name, file)
                break

        if custom_shapefile_path:
            st.success("✅ Shapefile cargado correctamente.")
        else:
            st.error("❌ No se encontró ningún archivo .shp válido en el ZIP.")

    # ──────────────────────────────
    st.markdown('<div class="hb-divider"></div>', unsafe_allow_html=True)
    # Dividimos el contenido en dos columnas
    row1 = st.columns([2, 2])
    row2 = st.columns([2, 2])

    with row1[0]:
        st.subheader(t("map.title"))
        map_embalses = geemap.Map(center=[42.0, 0.5], zoom=8)
        cargar_y_mostrar_embalses(
            map_embalses,
            shapefile_path=custom_shapefile_path if custom_shapefile_path else "shapefiles/embalses_hiblooms.shp",
            nombre_columna="NOMBRE"
        )
        folium_static(map_embalses, width=1000, height=600)

    with row1[1]:
        st.subheader(t("reservoir.pick.h"))
        nombres_embalses = obtener_nombres_embalses(custom_shapefile_path) if custom_shapefile_path else obtener_nombres_embalses()
        reservoir_name = st.selectbox(t("reservoir.pick.label"), nombres_embalses)

        if reservoir_name:
            gdf = load_reservoir_shapefile(reservoir_name, shapefile_path=custom_shapefile_path) if custom_shapefile_path else load_reservoir_shapefile(reservoir_name)
            if gdf is not None:
                aoi = gdf_to_ee_geometry(gdf)
                st.subheader(t("poi.h"))

                pois_embalse = {}
                
                if reservoir_name in puntos_interes:
                    st.success(t("poi.ok"))
                    pois_embalse = puntos_interes[reservoir_name]
                else:
                    st.warning(t("poi.none"))
                    archivo_pois = st.file_uploader("Sube un archivo CSV con los puntos de interés", type=["csv"])
                
                    if archivo_pois is not None:
                        try:
                            df_pois = pd.read_csv(archivo_pois)
                            columnas_esperadas = {"nombre", "latitud", "longitud"}
                            columnas_archivo = set(df_pois.columns.str.lower().str.strip())
                            
                            if columnas_esperadas.issubset(columnas_archivo):
                                # Renombrar columnas ignorando mayúsculas/minúsculas y espacios
                                columnas_mapeo = {col: col.lower().strip() for col in df_pois.columns}
                                df_pois = df_pois.rename(columns=columnas_mapeo)
                
                                pois_embalse = {
                                    row["nombre"]: (row["latitud"], row["longitud"]) for _, row in df_pois.iterrows()
                                }
                                puntos_interes[reservoir_name] = pois_embalse
                                st.success("Puntos cargados correctamente.")
                            else:
                                st.error("❌ El archivo debe tener columnas llamadas exactamente: 'nombre', 'latitud' y 'longitud'.")
                        except Exception as e:
                            st.error(f"❌ Error al leer el archivo: {e}")
                
                if pois_embalse:
                    st.markdown(t("poi.active"))
                    st.dataframe(pd.DataFrame([
                        {"nombre": nombre, "latitud": lat, "longitud": lon} for nombre, (lat, lon) in pois_embalse.items()
                    ]))

                # Slider de nubosidad
                st.subheader(t("cloud.h"))
                max_cloud_percentage = st.selectbox(
                    t("cloud.label"),
                    options=[60, 80, 100],
                    index=0
                )
                if max_cloud_percentage == 100:
                    st.info(t("cloud.100"))

                # Selección de intervalo de fechas
                st.subheader(t("dates.h"))
                date_range = st.date_input(t("dates.label"), value=(datetime.today() - timedelta(days=15), datetime.today()),
                                           min_value=datetime(2017, 7, 1), max_value=datetime.today(), format="DD-MM-YYYY")

                # Extraer fechas seleccionadas
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    start_date, end_date = date_range
                else:
                    start_date, end_date = datetime(2017, 7, 1), datetime.today()

                start_date = start_date.strftime('%Y-%m-%d')
                end_date = end_date.strftime('%Y-%m-%d')

                # Selección de índices
                st.subheader(t("idx.h"))
                available_indices = get_available_indices_for_reservoir(reservoir_name)
                selected_indices = st.multiselect(t("idx.label"), available_indices)
                with st.expander(t("idx.help")):
                    if lang()=="es":
                        st.markdown("""
                        - **MCI (Maximum Chlorophyll Index):** Detecta altas concentraciones de clorofila-a, útil para identificar blooms intensos.
                        - **PCI_B5/B4:** Relación espectral entre el infrarrojo cercano (B5) y el rojo (B4), es un buen indicador de ficocianina para todo tipo de embalses, pero no proporciona concentraciones directas.
                        - **NDCI_ind (Normalized Difference Chlorophyll Index):** Relación normalizada entre bandas del rojo e infrarrojo cercano. Se asocia a clorofila-a.
                        - **UV_PC_Gral_cal:** Estimación cuantitativa general de ficocianina basada en la relación espectral entre el infrarrojo cercano (B5) y el rojo (B4). Ajustada mediante una función exponencial, proporciona concentraciones aproximadas de ficocianina en µg/L. Desarrollado por la Universidad de Valencia a partir de datos del estudio en embalses de la cuenca del Ebro (Pérez-González et al., 2021).
                        - **PC_Val_cal (Ficocianina en El Val):** Estimador cuantitativo de ficocianina, un pigmento exclusivo de cianobacterias. Basado en la relación espectral entre el infrarrojo cercano y el rojo, ha sido ajustado a partir de mediciones de ficocianina en el Embalse de El Val.
                        - **Chla_Val_cal:** Calibración cuantitativa de clorofila-a derivada del NDCI mediante ajuste exponencial a partir de mediciones en el embalse de El Val.
                        - **Chla_Bellus_cal:** Estimación cuantitativa de clorofila-a específicamente calibrada para el embalse de Bellús.
                        - **PC_Bellus_cal (Ficocianina Bellús):** Ajuste específico para el embalse de Bellús, basado en la fórmula empírica derivada de la relación espectral MCI. Se estima la concentración de ficocianina en µg/L.
                        """)
                    else:
                        st.markdown("""- **MCI** ... (traducción EN del resumen)""")

                # Botón fuera del if
                # ── BOTÓN DE CÁLCULO ASÍNCRONO ──────────────────────────────────────────
                calcular = st.button(t("btn.compute"))

                # Separador siempre visible
    st.markdown('<div class="hb-divider"></div>', unsafe_allow_html=True)

                # Al pulsar el botón: construir el payload y enviarlo a la API de jobs
    if calcular:
                    if not selected_indices:
                        st.warning("⚠️ Selecciona al menos un índice antes de calcular.")
                    else:
                        _run_config = {
                            "workflow": "visualization",
                            "reservoir": reservoir_name,
                            "start_date": start_date,
                            "end_date": end_date,
                            "max_cloud_pct": int(max_cloud_percentage),
                            "indices": selected_indices,
                            "aoi_geojson": gdf.to_crs(epsg=4326).to_json(),
                            "puntos_interes": {
                                k: list(v) for k, v in puntos_interes.get(reservoir_name, {}).items()
                            },
                        }
                        try:
                            _resp = _requests.post(
                                f"{_API_URL}/jobs/submit",
                                json=_run_config,
                                timeout=60,
                            )
                            if _resp.ok:
                                _job_id = _resp.json()["job_id"]
                                st.session_state["viz_job_id"] = _job_id
                                st.session_state.pop("viz_job_results", None)
                                st.session_state.pop("data_time", None)
                                st.session_state["cloud_results"] = []
                                st.session_state["used_cloud_results"] = []
                                st.success(f"✅ Cálculo enviado (job `{_job_id}`). Los resultados aparecerán aquí en cuanto estén listos.")
                            else:
                                st.error(f"❌ Error al enviar el job: {_resp.status_code} – {_resp.text}")
                        except Exception as _e:
                            st.error(f"❌ No se pudo conectar con la API de jobs: {_e}")

    # ── PANEL DE ESTADO / POLLING ────────────────────────────────────────────────
    # Usando st.fragment para que solo se recargue esta sección, no toda la página
    if "viz_job_id" in st.session_state and "viz_job_results" not in st.session_state:

        @st.fragment(run_every=5)
        def _polling_fragment():
            _job_id = st.session_state.get("viz_job_id")
            if not _job_id:
                return
            try:
                _status = _requests.get(
                    f"{_API_URL}/jobs/{_job_id}/status", timeout=5
                ).json()
            except Exception:
                _status = {"state": "unknown"}

            _state = _status.get("state", "unknown")

            if _state == "running":
                _pct  = _status.get("progress", 0)
                _step = _status.get("step", "Procesando…")
                st.progress(_pct / 100, text=f"⏳ {_step}")

            elif _state == "done":
                _res = _status.get("results", {})
                st.session_state["viz_job_results"] = _res
                st.session_state["data_time"]          = _res.get("data_time", [])
                st.session_state["cloud_results"]      = _res.get("cloud_results", [])
                st.session_state["used_cloud_results"] = _res.get("used_cloud_results", [])
                st.session_state["available_dates"]    = _res.get("available_dates", [])
                st.session_state["selected_indices"]   = _res.get("selected_indices", [])
                st.session_state["urls_exportacion"]   = _res.get("urls_exportacion", [])
                del st.session_state["viz_job_id"]
                st.rerun()

            elif _state == "error":
                st.error(f"❌ El workflow falló: {_status.get('error', 'error desconocido')}")
                del st.session_state["viz_job_id"]

            else:
                st.info("⏳ Esperando respuesta del servidor de jobs…")

        _polling_fragment()

    # ── RENDERIZADO DE RESULTADOS ────────────────────────────────────────────────
    # Se activa cuando los resultados ya están en session_state
    # (igual que antes, pero leyendo de session_state en vez de calcular en vivo)
    _data_time       = st.session_state.get("data_time", [])
    _selected_indices = st.session_state.get("selected_indices", selected_indices)
    _available_dates = st.session_state.get("available_dates", [])

    if _data_time or _available_dates:
        df_time = pd.DataFrame(_data_time)

        # Timeline de fechas disponibles
        if _available_dates:
            st.subheader(t("avail.h"))
            # Deduplicar por fecha (sin hora) y ordenar
            _fechas_unicas = sorted(set(
                pd.to_datetime(f).strftime("%Y-%m-%d") for f in _available_dates
            ))

            # Renderizar como pills HTML
            _style = "background:#5297d2;color:white;border-radius:20px;padding:6px 16px;font-size:14px;font-weight:500;white-space:nowrap;display:inline-block;"
            _pills = "".join(
                "<span style='" + _style + "'>📅 " + pd.to_datetime(_f).strftime("%d %b %Y") + "</span>"
                for _f in _fechas_unicas
            )
            _wrap = "<div style='display:flex;flex-wrap:wrap;gap:10px;padding:12px 0;'>" + _pills + "</div>"
            st.markdown(_wrap, unsafe_allow_html=True)

        with row2[1]:
            # Leyenda de índices y capas
            with st.expander(t("legend.exp"), expanded=False):
                generar_leyenda(_selected_indices)

            # Tabla de nubosidad estimada por imagen
            if st.session_state.get("used_cloud_results"):
                with st.expander(t("cloud.exp"), expanded=False):
                    df_results = pd.DataFrame(st.session_state["used_cloud_results"]).copy()
                    df_results["Fecha"] = pd.to_datetime(df_results["Fecha"], errors="coerce").dt.strftime("%d-%m-%Y")
                    df_results = df_results.rename(columns={
                        "Fecha": t("col.date"),
                        "Hora": t("col.time"),
                        "Nubosidad aproximada (%)": t("col.cloud"),
                    })
                    st.dataframe(df_results)

            # Gráfico de media diaria del embalse
            with st.expander(t("mean.exp"), expanded=False):
                if not df_time.empty:
                    df_media = df_time[df_time["Point"] == "Media_Embalse"].copy()
                    df_media["Date"] = pd.to_datetime(df_media["Date"], errors="coerce")
                    for indice in _selected_indices:
                        if indice in df_media.columns:
                            df_indice = df_media[["Date", indice]].dropna()
                            y_title = t("axis.conc") if "cal" in indice.lower() else t("axis.idx")
                            chart = alt.Chart(df_indice).mark_bar().encode(
                                x=alt.X("Date:T", title=t("col.date"), axis=alt.Axis(format="%d-%b", labelAngle=0)),
                                y=alt.Y(f"{indice}:Q", title=y_title),
                                tooltip=[
                                    alt.Tooltip("Date:T", title=t("col.date")),
                                    alt.Tooltip(f"{indice}:Q", title=indice),
                                ],
                            ).properties(title=f"{t('mean.index')}: {indice}", width=500, height=300)
                            st.altair_chart(chart, use_container_width=True)

        with row2[0]:
            # Mapas por fecha — disponibles si el worker devolvió tile_urls en results
            _tile_data = st.session_state.get("viz_job_results", {}).get("tile_urls", [])
            _scl_palette = {
                1: "#ff0004", 2: "#000000", 3: "#8B4513", 4: "#00FF00",
                5: "#FFD700", 6: "#0000FF", 7: "#F4EEEC", 8: "#C8C2C0",
                9: "#706C6B", 10: "#87CEFA", 11: "#00FFFF",
            }
            _scl_colors = [_scl_palette[i] for i in sorted(_scl_palette.keys())]
            for _entry in _tile_data:
                _image_date_fmt = _entry.get("date", "")
                with st.expander(f"{t('map.index.for')} {_image_date_fmt}"):
                    if gdf is not None:
                        _gdf_4326 = gdf.to_crs(epsg=4326)
                        _map_center = [
                            _gdf_4326.geometry.centroid.y.mean(),
                            _gdf_4326.geometry.centroid.x.mean(),
                        ]
                    else:
                        _map_center = [41.0, -1.5]
                    _map_indices = geemap.Map(center=_map_center, zoom=13)
                    # Restringir la vista a los bounds del embalse
                    _bounds = _gdf_4326.total_bounds  # (minx, miny, maxx, maxy)
                    _map_indices.fit_bounds([[_bounds[1], _bounds[0]], [_bounds[3], _bounds[2]]])
                    for _lname, _url in _entry.get("layers", {}).items():
                        folium.raster_layers.TileLayer(
                            tiles=_url,
                            name=_lname,
                            overlay=True,
                            control=True,
                            show=(_lname == t("map.rgb")),
                            attr="Copernicus Sentinel-2, processed by GEE",
                        ).add_to(_map_indices)
                    _poi_group = folium.FeatureGroup(name=t("map.poi"), show=False)
                    for _pname, (_plat, _plon) in puntos_interes.get(reservoir_name, {}).items():
                        folium.Marker(
                            location=[_plat, _plon],
                            popup=_pname,
                            tooltip=_pname,
                            icon=folium.Icon(color="red", icon="info-sign"),
                        ).add_to(_poi_group)
                    _poi_group.add_to(_map_indices)
                    folium.LayerControl(collapsed=False, position="topright").add_to(_map_indices)
                    folium_static(_map_indices)

        # Gráficos de distribución por clases (requiere image_list en session_state)
        if "image_list" in st.session_state and st.session_state["image_list"]:
            with st.expander("📊 Distribución diaria por clases del índice en el embalse", expanded=False):
                        with tab4:
                            st.subheader("Tablas de Índices Calculados")
                        
                            if not df_time.empty:
                                df_time = df_time.copy()
                        
                                # Renombrar la columna 'Point' a 'Ubicación'
                                df_time.rename(columns={"Point": "Ubicación"}, inplace=True)
                        
                                # Crear una única columna 'Fecha' en formato datetime para ordenar
                                if "Fecha" not in df_time.columns:
                                    posibles_fechas = ["Date", "Fecha-hora", "Fecha_dt"]
                                    for col in posibles_fechas:
                                        if col in df_time.columns:
                                            df_time["Fecha"] = pd.to_datetime(df_time[col], errors='coerce')
                                            break
                        
                                # Verificar que 'Fecha' existe y eliminar duplicados
                                if "Fecha" not in df_time.columns:
                                    st.error("❌ No se encontró ninguna columna de fecha válida.")
                                    st.stop()
                        
                                # Ordenar por 'Ubicación' y 'Fecha' (orden cronológico)
                                df_time = df_time.dropna(subset=["Fecha"]).sort_values(by=["Ubicación", "Fecha"])
                        
                                # Convertir la fecha a texto para visualización
                                df_time["Fecha"] = df_time["Fecha"].dt.strftime("%d-%m-%Y %H:%M")
                        
                                # Eliminar columnas de fecha duplicadas si existen
                                columnas_fecha = ["Date", "Fecha-hora", "Fecha_dt"]
                                df_time.drop(columns=[col for col in columnas_fecha if col in df_time.columns], errors='ignore', inplace=True)
                        
                                # Ordenar las columnas
                                columnas = list(df_time.columns)
                                orden = ["Ubicación", "Fecha", "Tipo"]
                                otras = [col for col in columnas if col not in orden]
                                columnas_ordenadas = orden + otras
                                df_time = df_time[columnas_ordenadas]
                        
                                # Dividir en puntos de interés y medias del embalse
                                df_medias = df_time[df_time["Ubicación"] == "Media_Embalse"]
                                df_puntos = df_time[df_time["Ubicación"] != "Media_Embalse"]
                        
                                # Mostrar las tablas corregidas
                                if not df_puntos.empty:
                                    st.markdown("### 📌 Datos en los puntos de interés")
                                    st.dataframe(df_puntos.reset_index(drop=True))
                        
                                if not df_medias.empty:
                                    st.markdown("### 💧 Datos de medias del embalse")
                                    st.dataframe(df_medias.reset_index(drop=True))
                            else:
                                st.warning("No hay datos disponibles. Primero realiza el cálculo en la pestaña de Visualización.")                                                   
                                                            
with tab5:
                            st.subheader("📈 Modo rápido: generación de gráficas")
                        
                            st.info("Este modo solo genera gráficas a partir de los parámetros seleccionados, sin mapas ni exportaciones.")
                        
                            # Selección de embalse
                            nombres_embalses = obtener_nombres_embalses()
                            reservoir_name = st.selectbox("Selecciona un embalse:", nombres_embalses, key="graficas_embalse")
                        
                            if reservoir_name:
                                gdf = load_reservoir_shapefile(reservoir_name)
                                if gdf is not None:
                                    aoi = gdf_to_ee_geometry(gdf)
                        
                                    max_cloud_percentage = st.slider("Porcentaje máximo de nubosidad permitido:", 0, 100, 10, key="graficas_nubosidad")
                        
                                    date_range = st.date_input(
                                        "Selecciona el rango de fechas:",
                                        value=(datetime.today() - timedelta(days=15), datetime.today()),
                                        min_value=datetime(2017, 7, 1),
                                        max_value=datetime.today(),
                                        key="graficas_fecha"
                                    )
                        
                                    if isinstance(date_range, tuple) and len(date_range) == 2:
                                        start_date, end_date = date_range
                                    else:
                                        start_date, end_date = datetime(2017, 7, 1), datetime.today()
                        
                                    start_date = start_date.strftime('%Y-%m-%d')
                                    end_date = end_date.strftime('%Y-%m-%d')
                        
                                    available_indices = get_available_indices_for_reservoir(reservoir_name)
                                    selected_indices = st.multiselect("Selecciona los índices a visualizar:", available_indices, key="graficas_indices")
          
                                    if st.button("Ejecutar modo rápido"):
                                        allowed = set(get_available_indices_for_reservoir(reservoir_name))
                                        selected_indices = [i for i in selected_indices if i in allowed]
                                        st.session_state["data_time"] = []
                                        # Mapeo de nombres para los CSV precalculados
                                        csv_name_map = {
                                                "El Val": "val",
                                                "Bellús": "bellus"
                                        }
                                        reservoir_key = csv_name_map.get(reservoir_name, None)
                                            
                                        # Usar CSV precalculado si procede
                                        if reservoir_key in ["val", "bellus"] and max_cloud_percentage == 60:
                                                url_csv = f"https://{BUCKET_NAME}.s3.amazonaws.com/fechas_validas/{reservoir_key}_60.csv"
                                                try:
                                                    df_csv = pd.read_csv(url_csv)
                                                    available_dates = pd.to_datetime(df_csv["fechas"])
                                                    available_dates = available_dates[
                                                        (available_dates >= pd.to_datetime(start_date)) &
                                                        (available_dates <= pd.to_datetime(end_date))
                                                    ]
                                                except Exception as e:
                                                    st.warning(f"No se pudo cargar el CSV precalculado para {reservoir_name}: {e}")
                                                    available_dates = get_available_dates(aoi, start_date, end_date, max_cloud_percentage)
                                        else:
                                                available_dates = get_available_dates(aoi, start_date, end_date, max_cloud_percentage)

                                        if not available_dates:
                                                st.warning("No se encontraron imágenes en ese rango de fechas.")
                                                st.stop()
                        
                                        data_time = []
                                        clorofila_indices = {"MCI", "NDCI_ind", "Chla_Val_cal", "Chla_Bellus_cal"}
                                        ficocianina_indices = {"UV_PC_Gral_cal""PC_Val_cal", "PCI_B5/B4","PC_Bellus_cal"}
                        
                                        hay_clorofila = any(i in selected_indices for i in clorofila_indices)
                                        hay_ficocianina = any(i in selected_indices for i in ficocianina_indices)
                        
                                        if reservoir_name.lower() == "val" and hay_ficocianina:
                                            urls = [
                                                "https://drive.google.com/uc?id=1-FpLJpudQd69r9JxTbT1EhHG2swASEn-&export=download",
                                                "https://drive.google.com/uc?id=1w5vvpt1TnKf_FN8HaM9ZVi3WSf0ibxlV&export=download"
                                            ]
                                            df_list = [cargar_csv_desde_url(u) for u in urls]
                                            df_list = [df for df in df_list if not df.empty]
                                            if df_list:
                                                df_fico = pd.concat(df_list).sort_values('Fecha-hora')
                                                start_dt = pd.to_datetime(start_date)
                                                end_dt = pd.to_datetime(end_date)
                                                df_filtrado = df_fico[(df_fico['Fecha-hora'] >= start_dt) & (df_fico['Fecha-hora'] <= end_dt)]
                                                for _, row in df_filtrado.iterrows():
                                                    data_time.append({
                                                        "Point": "SAICA_Val",
                                                        "Date": row["Fecha-hora"],
                                                        "Ficocianina (µg/L)": row["Ficocianina (µg/L)"],
                                                        "Tipo": "Valor Real"
                                                    })
                        
                                        if reservoir_name.lower() == "bellus" and (hay_clorofila or hay_ficocianina):
                                            url_fico = "https://drive.google.com/uc?id=1jeTpJfPTTKORN3iIprh6P_RPXPu16uDa&export=download"
                                            url_cloro = "https://drive.google.com/uc?id=17-jtO6mbjfj_CMnsMo_UX2RQ7IM_0hQ4&export=download"
                                            df_fico = cargar_csv_desde_url(url_fico)
                                            df_cloro = cargar_csv_desde_url(url_cloro)
                        
                                            for col in df_fico.columns:
                                                if "pc_ivf" in col.lower():
                                                    df_fico.rename(columns={col: "Ficocianina (µg/L)"}, inplace=True)
                                            for col in df_cloro.columns:
                                                if "chla_ivf" in col.lower():
                                                    df_cloro.rename(columns={col: "Clorofila (µg/L)"}, inplace=True)
                        
                                            if not df_fico.empty and not df_cloro.empty:
                                                df_bellus = pd.merge(df_fico, df_cloro, on="Fecha-hora", how="outer")
                                                df_bellus = df_bellus.sort_values("Fecha-hora")
                                                start_dt = pd.to_datetime(start_date)
                                                end_dt = pd.to_datetime(end_date)
                                                df_bellus_filtrado = df_bellus[(df_bellus["Fecha-hora"] >= start_dt) & (df_bellus["Fecha-hora"] <= end_dt)]
                                                for _, row in df_bellus_filtrado.iterrows():
                                                    entry = {"Point": "Sonda-Bellús", "Date": row["Fecha-hora"], "Tipo": "Real"}
                                                    if hay_ficocianina and pd.notna(row.get("Ficocianina (µg/L)")):
                                                        entry["Ficocianina (µg/L)"] = row["Ficocianina (µg/L)"]
                                                    if hay_clorofila and pd.notna(row.get("Clorofila (µg/L)")):
                                                        entry["Clorofila (µg/L)"] = row["Clorofila (µg/L)"]
                                                    if "Ficocianina (µg/L)" in entry or "Clorofila (µg/L)" in entry:
                                                        data_time.append(entry)
                        
                                        for day in available_dates:
                                            _, indices_image, _, _cloud, _cov = process_sentinel2(aoi, day, max_cloud_percentage, selected_indices)
                                            if indices_image is None:
                                                continue

                                            if reservoir_name in puntos_interes and puntos_interes[reservoir_name]: 
                                                for point_name, (lat, lon) in puntos_interes[reservoir_name].items():
                                                    values = get_values_at_point(lat, lon, indices_image, selected_indices)
                                                    registro = {"Point": point_name, "Date": day, "Tipo": "Valor Estimado"}
                                                    for i in selected_indices:
                                                        if i in values and values[i] is not None:
                                                            registro[i] = values[i]
                                                    if any(i in registro for i in selected_indices):
                                                        data_time.append(registro)
                            
                                                for i in selected_indices:
                                                    media_valor = calcular_media_diaria_embalse(indices_image, i, aoi)
                                                    if media_valor is not None:
                                                        data_time.append({
                                                            "Point": "Media_Embalse",
                                                            "Date": day,
                                                            i: media_valor,
                                                            "Tipo": "Valor Estimado"
                                                        })
                                        df_time = pd.DataFrame(data_time)
                                        if df_time.empty:
                                            st.warning("No se generaron datos válidos.")
                                            st.stop()
                        
                                        st.session_state["data_time"] = data_time
                                        st.success("✅ Datos procesados correctamente. Mostrando gráficas:")
                                        df_time["Fecha_dt"] = pd.to_datetime(df_time["Date"], errors='coerce')
                        
                                        with st.expander("📊 Evolución de la media diaria del embalse", expanded=True):
                                            df_media = df_time[df_time["Point"] == "Media_Embalse"]
                                            for i in selected_indices:
                                                if i in df_media.columns:
                                                    df_ind = df_media[["Fecha_dt", i]].dropna()
                                                    chart = alt.Chart(df_ind).mark_bar().encode(
                                                        x=alt.X("Fecha_dt:T", title="Fecha"),
                                                        y=alt.Y(f"{i}:Q", title="Concentración"),
                                                        tooltip=["Fecha_dt", i]
                                                    ).properties(title=f"{i} – Media embalse")
                                                    st.altair_chart(chart, use_container_width=True)
                        
                                        with st.expander("📍 Valores por punto de interés", expanded=True):
                                            for point in df_time["Point"].unique():
                                                if point != "Media_Embalse":
                                                    df_p = df_time[df_time["Point"] == point]
                                                    df_melt = df_p.melt(id_vars=["Point", "Fecha_dt"],
                                                                        value_vars=selected_indices,
                                                                        var_name="Índice", value_name="Valor")
                                                    chart = alt.Chart(df_melt).mark_line(point=True).encode(
                                                        x=alt.X("Fecha_dt:T", title="Fecha"),
                                                        y=alt.Y("Valor:Q", title="Valor"),
                                                        color="Índice:N",
                                                        tooltip=["Fecha_dt", "Índice", "Valor"]
                                                    ).properties(title=f"{point} – evolución de índices")
                                                    st.altair_chart(chart, use_container_width=True)
                                        # Mostrar tablas de resultados igual que en la pestaña "Tablas"
                                        st.subheader("📄 Resultados en tabla")
                                        
                                        # Copia del DataFrame y limpieza básica
                                        df_tabla = df_time.copy()
                                        df_tabla.rename(columns={"Point": "Ubicación"}, inplace=True)
                                        df_tabla["Fecha"] = pd.to_datetime(df_tabla["Date"], errors='coerce').dt.strftime("%d-%m-%Y %H:%M")
                                        df_tabla.drop(columns=["Date", "Fecha_formateada", "Fecha_dt", "Fecha-hora"], errors='ignore', inplace=True)
                                        
                                        # Agrupar valores medios si hay duplicados
                                        df_medias = df_tabla[df_tabla["Ubicación"] == "Media_Embalse"]
                                        df_otros = df_tabla[df_tabla["Ubicación"] != "Media_Embalse"]
                                        
                                        if not df_medias.empty:
                                            columnas_valor = [col for col in df_medias.columns if col not in ["Ubicación", "Fecha", "Tipo"]]
                                            df_medias = df_medias.groupby(["Ubicación", "Fecha", "Tipo"], as_index=False).agg({col: "max" for col in columnas_valor})
                                        
                                        df_tabla = pd.concat([df_medias, df_otros], ignore_index=True)
                                        
                                        # Ordenar columnas
                                        columnas = list(df_tabla.columns)
                                        orden = ["Ubicación", "Fecha", "Tipo"]
                                        otras = [col for col in columnas if col not in orden]
                                        columnas_ordenadas = orden + otras
                                        df_tabla = df_tabla[columnas_ordenadas]
                                        
                                        # Separar y mostrar
                                        df_puntos = df_tabla[df_tabla["Ubicación"] != "Media_Embalse"]
                                        df_medias = df_tabla[df_tabla["Ubicación"] == "Media_Embalse"]
                                        
                                        if not df_puntos.empty:
                                            st.markdown("### 📌 Datos en los puntos de interés")
                                            st.dataframe(df_puntos.reset_index(drop=True))
                                        
                                        if not df_medias.empty:
                                            st.markdown("### 💧 Datos de medias del embalse")
                                            st.dataframe(df_medias.reset_index(drop=True))
