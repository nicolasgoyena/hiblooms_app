# encoding: utf-8

import ee
import streamlit as st
from datetime import datetime
# ==== i18n: idioma ES/EN con URL ?lang= y session_state ====


_LANGS = ["es", "en"]

_STR = {
    "es": {
        "meta.title": "HIBLOOMS – Visor de embalses",
        "ui.language": "Idioma",
        "tabs.intro": "Introducción",
        "tabs.map": "Visualización",
        "tabs.tables": "Tablas",
        "tabs.quick": "Modo rápido",
        "hero.l1": "Visor de indicadores de la calidad del agua en embalses españoles:",
        "hero.l2": "Proyecto HIBLOOMS",

        "upload.shp.h": "🔄 Cargar shapefile propio con todos los embalses de interés (opcional)",
        "upload.shp.i": "📄 Asegúrate de que el shapefile contiene una columna llamada **'NOMBRE'** con el nombre de cada embalse.",
        "upload.shp.zip": "Sube un archivo ZIP con tu shapefile de embalses",

        "map.title": "Mapa de Embalses",
        "reservoir.pick.h": "Selección de Embalse",
        "reservoir.pick.label": "Selecciona un embalse:",
        "poi.h": "Puntos de interés",
        "poi.ok": "Puntos de interés por defecto disponibles para este embalse.",
        "poi.none": "Este embalse no tiene puntos de interés por defecto. Puedes subir un archivo CSV con columnas llamadas exactamente: 'nombre', 'latitud' y 'longitud'.",
        "poi.csv": "Sube un archivo CSV con los puntos de interés",
        "poi.active": "**Puntos de interés activos:**",

        "cloud.h": "Selecciona un porcentaje máximo de nubosidad:",
        "cloud.label": "Es recomendable elegir hasta el 60%. Si quieres ver más imágenes, aumenta la tolerancia:",
        "cloud.100": "🔁 Has seleccionado un 100 % de nubosidad: se mostrarán todas las imágenes del periodo. Se estimará igualmente la nubosidad de cada imagen.",

        "dates.h": "Selecciona el intervalo de fechas:",
        "dates.label": "Rango de fechas:",

        "idx.h": "Selecciona los índices a visualizar:",
        "idx.label": "Selecciona uno o varios índices para visualizar y analizar:",
        "idx.help": "ℹ️ ¿Qué significa cada índice?",
        "btn.compute": "Calcular y mostrar resultados",

        "spinner.img": "🕒 Analizando imagen del",
        "spinner.proc": "Calculando fechas disponibles...",
        "warn.no_imgs": "⚠️ No se han encontrado imágenes dentro del rango y porcentaje de nubosidad seleccionados.",

        "avail.h": "📅 Fechas disponibles dentro del rango seleccionado:",
        "legend.exp": "🗺️ Leyenda de índices y capas",
        "cloud.exp": "☁️ Nubosidad estimada por imagen",
        "mean.exp": "📊 Evolución de la media diaria de concentraciones del embalse",
        "dist.exp": "📊 Distribución diaria por clases del índice en el embalse",
        "legend.title": "📌 Leyenda de Índices y Capas",

        "map.rgb": "RGB",
        "map.scl": "SCL - Clasificación de Escena",
        "map.cloudprob": "Probabilidad de Nubes (MSK_CLDPRB)",
        "map.poi": "Puntos de Interés",
        "map.index.for": "📅 Mapa de Índices para",
        "mean.index": "🧪 Índice",
        "axis.conc": "Concentración (μg/L)",
        "axis.idx": "Valor del índice",

        "col.date": "Fecha",
        "col.time": "Hora",
        "col.cloud": "Nubosidad aproximada (%)",

        # SCL labels
        "scl.1": "Píxeles saturados/defectuosos",
        "scl.2": "Área oscura",
        "scl.3": "Sombras de nube",
        "scl.4": "Vegetación",
        "scl.5": "Suelo desnudo",
        "scl.6": "Agua",
        "scl.7": "Prob. baja de nubes / No clasificada",
        "scl.8": "Prob. media de nubes",
        "scl.9": "Prob. alta de nubes",
        "scl.10": "Cirro",
        "scl.11": "Nieve o hielo",
    },
    "en": {
        "meta.title": "HIBLOOMS – Reservoir viewer",
        "ui.language": "Language",
        "tabs.intro": "Introduction",
        "tabs.map": "Map",
        "tabs.tables": "Tables",
        "tabs.quick": "Quick mode",
        "hero.l1": "Water-quality indicator viewer for Spanish reservoirs:",
        "hero.l2": "HIBLOOMS Project",

        "upload.shp.h": "🔄 Load your own shapefile with all reservoirs of interest (optional)",
        "upload.shp.i": "📄 Make sure the shapefile contains a **'NOMBRE'** column with each reservoir name.",
        "upload.shp.zip": "Upload a ZIP file with your reservoir shapefile",

        "map.title": "Reservoir map",
        "reservoir.pick.h": "Reservoir selection",
        "reservoir.pick.label": "Select a reservoir:",
        "poi.h": "Points of interest",
        "poi.ok": "Default points of interest are available for this reservoir.",
        "poi.none": "No default POIs for this reservoir. You can upload a CSV with columns exactly named: 'nombre', 'latitud', 'longitud'.",
        "poi.csv": "Upload a CSV with points of interest",
        "poi.active": "**Active points of interest:**",

        "cloud.h": "Pick a maximum cloud percentage:",
        "cloud.label": "Values up to 60% are recommended. Increase tolerance to see more images:",
        "cloud.100": "🔁 You selected 100% clouds: all images in the period will be included. We still estimate cloud probability per image.",

        "dates.h": "Select the date range:",
        "dates.label": "Date range:",

        "idx.h": "Pick the indices to display:",
        "idx.label": "Select one or more indices for visualization and analysis:",
        "idx.help": "ℹ️ What does each index mean?",
        "btn.compute": "Compute & show results",

        "spinner.img": "🕒 Analyzing image from",
        "spinner.proc": "Calculating available dates...",
        "warn.no_imgs": "⚠️ No images found within the selected range and cloud threshold.",

        "avail.h": "📅 Available dates in the selected range:",
        "legend.exp": "🗺️ Legend for indices and layers",
        "cloud.exp": "☁️ Estimated cloud per image",
        "mean.exp": "📊 Daily mean evolution for the reservoir",
        "dist.exp": "📊 Daily per-class distribution in the reservoir",
        "legend.title": "📌 Indices & Layers Legend",

        "map.rgb": "RGB",
        "map.scl": "SCL - Scene Classification",
        "map.cloudprob": "Cloud probability (MSK_CLDPRB)",
        "map.poi": "Points of Interest",
        "map.index.for": "📅 Index maps for",
        "mean.index": "🧪 Index",
        "axis.conc": "Concentration (μg/L)",
        "axis.idx": "Index value",

        "col.date": "Date",
        "col.time": "Time",
        "col.cloud": "Approx. cloud (%)",

        # SCL labels
        "scl.1": "Saturated/defective pixels",
        "scl.2": "Dark area pixels",
        "scl.3": "Cloud shadows",
        "scl.4": "Vegetation",
        "scl.5": "Bare soil",
        "scl.6": "Water",
        "scl.7": "Low cloud probability / Unclassified",
        "scl.8": "Medium cloud probability",
        "scl.9": "High cloud probability",
        "scl.10": "Cirrus",
        "scl.11": "Snow or ice",
    },
}

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
def cargar_fechas_csv(url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(url)

        if 'fecha' in df.columns:
            df.rename(columns={'fecha': 'Fecha'}, inplace=True)
        else:
            st.warning("❌ El CSV no contiene la columna esperada 'fecha'.")
            return pd.DataFrame()

        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df.dropna(subset=['Fecha'], inplace=True)

        return df
    except Exception as e:
        st.warning(f"⚠️ Error al cargar el CSV desde {url}: {e}")
        return pd.DataFrame()

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

def get_available_dates(aoi, start_date, end_date, max_cloud_percentage):

    sentinel2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
    )

    def compute_metrics(image):

        scl = image.select("SCL")

        cloud_mask = scl.eq(7).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
        valid_mask = scl.mask().And(scl.neq(4)).And(scl.neq(5))

        cloud_fraction = cloud_mask.updateMask(valid_mask).reduceRegion(
            ee.Reducer.mean(), aoi, 20, maxPixels=1e13
        ).get("SCL")

        total_pixels = ee.Image(1).clip(aoi).reduceRegion(
            ee.Reducer.count(), aoi, 20, maxPixels=1e13
        ).get("constant")

        valid_pixels = ee.Image(1).updateMask(image.select("B4").mask()).clip(aoi).reduceRegion(
            ee.Reducer.count(), aoi, 20, maxPixels=1e13
        ).get("constant")

        coverage = ee.Number(valid_pixels).divide(total_pixels).multiply(100)

        return image.set({
            "cloud_pct": ee.Number(cloud_fraction).multiply(100),
            "coverage_pct": coverage,
            "date_str": ee.Date(image.get("system:time_start")).format("YYYY-MM-dd"),
            "time_str": ee.Date(image.get("system:time_start")).format("HH:mm")
        })

    processed = sentinel2.map(compute_metrics)

    # 🔥 Traemos todo en una sola llamada
    dates = processed.aggregate_array("date_str").getInfo()
    times = processed.aggregate_array("time_str").getInfo()
    clouds = processed.aggregate_array("cloud_pct").getInfo()
    coverages = processed.aggregate_array("coverage_pct").getInfo()

    if not dates:
        st.warning(t("warn.no_imgs"))
        return []

    results_list = []

    for d, t_, c, cov in zip(dates, times, clouds, coverages):

        if (max_cloud_percentage == 100 or c <= max_cloud_percentage) and cov >= 50:

            results_list.append({
                "Fecha": d,
                "Hora": t_,
                "Nubosidad aproximada (%)": round(c, 2),
                "Cobertura (%)": round(cov, 2)
            })

    # eliminar duplicados por fecha
    unique = {r["Fecha"]: r for r in results_list}
    results_list = list(unique.values())

    st.session_state["cloud_results"] = results_list

    return sorted([r["Fecha"] for r in results_list])

def calcular_distribucion_area_por_clases(indices_image, index_name, aoi, bins):

    # SCL
    scl = indices_image.select("SCL")
    year = datetime.utcfromtimestamp(indices_image.get('system:time_start').getInfo() / 1000).year

    if year == 2018:
        mask_agua = scl.eq(6).Or(scl.eq(2))
    else:
        mask_agua = scl.eq(6)

    # Imagen del índice sobre agua
    imagen_indice = indices_image.select(index_name).updateMask(mask_agua)

    # AREA TOTAL DEL EMBALSE (solo una vez)
    pixel_area_total = ee.Image.pixelArea().updateMask(mask_agua)
    total_area = pixel_area_total.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=aoi,
        scale=20,
        maxPixels=1e13
    ).get("area")

    total_area_ha = ee.Number(total_area).divide(10000).getInfo()
    if not total_area_ha or total_area_ha == 0:
        return []

    results = []

    # Calcular el área para cada bin
    for i in range(len(bins) - 1):
        lower = bins[i]
        upper = bins[i+1]

        bin_mask = imagen_indice.gte(lower).And(imagen_indice.lt(upper))

        # pixel_area por bin
        pixel_area_bin = ee.Image.pixelArea().updateMask(bin_mask)

        bin_area = pixel_area_bin.reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=aoi,
            scale=20,
            maxPixels=1e13
        ).get("area")

        area_ha = ee.Number(bin_area).divide(10000).getInfo()
        if area_ha is None:
            area_ha = 0

        porcentaje = (area_ha / total_area_ha) * 100

        results.append({
            "rango": f"{lower}–{upper}",
            "area_ha": area_ha,
            "porcentaje": porcentaje
        })

    return results



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


def calcular_media_diaria_embalse(indices_image, index_name, aoi):
    """Calcula la media del índice dado sobre el embalse solo en píxeles de agua SCL == 6 (o SCL == 2 también para el año 2018)."""
    scl = indices_image.select('SCL')

    # Extraer la fecha de la imagen
    fecha_millis = indices_image.get('system:time_start').getInfo()
    fecha_dt = datetime.utcfromtimestamp(fecha_millis / 1000)
    year = fecha_dt.year

    if year == 2018:
        # Considerar SCL 6 (agua) y 2 (área oscura)
        mask_agua = scl.eq(6).Or(scl.eq(2))
    else:
        mask_agua = scl.eq(6)

    indice_filtrado = indices_image.select(index_name).updateMask(mask_agua)

    media = indice_filtrado.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=20,
        maxPixels=1e13
    ).get(index_name)

    return media.getInfo() if media is not None else None

def calculate_cloud_percentage(image, aoi):
    scl = image.select('SCL')
    
    # Crear máscara de nubes (SCL 7, 8, 9, 10) pero excluir vegetación (SCL 4) y suelo desnudo (SCL 5)
    cloud_mask_scl = scl.eq(7).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
    non_valid_mask = scl.eq(4).Or(scl.eq(5))  # Excluir vegetación y suelo desnudo
    
    # Aplicar máscara para excluir píxeles no válidos
    valid_pixels_mask = scl.mask().And(non_valid_mask.Not())

    # Calcular la fracción de nubes solo sobre píxeles válidos
    cloud_fraction_scl = cloud_mask_scl.updateMask(valid_pixels_mask).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=20,
        maxPixels=1e13
    ).get('SCL')

    cloud_mask_prob = image.select('MSK_CLDPRB').gte(10).updateMask(valid_pixels_mask)
    cloud_fraction_prob = cloud_mask_prob.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=20,
        maxPixels=1e13
    ).get('MSK_CLDPRB')

    scl_ok = cloud_fraction_scl is not None
    prob_ok = cloud_fraction_prob is not None

    if not scl_ok and not prob_ok:
        return None  # ❌ No se puede calcular nubosidad con ninguna de las bandas

    if scl_ok and prob_ok:
        return (
            ee.Number(cloud_fraction_scl).multiply(0.95)
            .add(ee.Number(cloud_fraction_prob).multiply(0.05))
            .multiply(100)
        )

    elif scl_ok:
        return ee.Number(cloud_fraction_scl).multiply(100)

    else:  # prob_ok
        return ee.Number(cloud_fraction_prob).multiply(100)

def calculate_coverage_percentage(image, aoi):
    """Devuelve el % del embalse cubierto por la imagen (basado en la banda B4)."""
    try:
        # Capa constante para total de píxeles dentro del embalse
        total_pixels = ee.Image(1).clip(aoi).reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=aoi,
            scale=20,
            maxPixels=1e13
        ).get("constant")

        # Píxeles con datos válidos (usamos máscara de B4 como indicador)
        valid_mask = image.select("B4").mask()
        valid_pixels = ee.Image(1).updateMask(valid_mask).clip(aoi).reduceRegion(
            reducer=ee.Reducer.count(),
            geometry=aoi,
            scale=20,
            maxPixels=1e13
        ).get("constant")

        if total_pixels is None or valid_pixels is None:
            return 0

        coverage = ee.Number(valid_pixels).divide(ee.Number(total_pixels)).multiply(100)
        return coverage.getInfo()

    except Exception as e:
        print(f"Error al calcular cobertura de imagen: {e}")
        return 0

def process_sentinel2(aoi, selected_date, max_cloud_percentage, selected_indices):

    with st.spinner("Procesando imágenes de Sentinel-2 para " + selected_date + "..."):

        selected_date_ee = ee.Date(selected_date)
        end_date_ee = selected_date_ee.advance(1, 'day')

        sentinel2 = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(aoi)
            .filterDate(selected_date_ee, end_date_ee)
        )

        # 🔥 Una sola llamada al servidor
        metadata_list = sentinel2.aggregate_array('system:time_start').getInfo()

        if not metadata_list:
            st.warning(f"No hay imágenes disponibles para la fecha {selected_date}")
            return None, None, None

        cloud_results_dict = {
            r["Fecha"]: r for r in st.session_state.get("cloud_results", [])
        }

        best_image = None
        selected_timestamp = None

        for timestamp in metadata_list:

            formatted_date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

            if formatted_date not in cloud_results_dict:
                continue

            cloud_score = cloud_results_dict[formatted_date]["Nubosidad aproximada (%)"]
            coverage = cloud_results_dict[formatted_date]["Cobertura (%)"]

            if coverage < 50 or cloud_score > max_cloud_percentage:
                continue

            # 🔥 No volvemos a filtrar, usamos la colección ya filtrada
            best_image = sentinel2.first()
            selected_timestamp = timestamp

            st.session_state.setdefault("used_cloud_results", []).append({
                "Fecha": formatted_date,
                "Hora": datetime.utcfromtimestamp(timestamp / 1000).strftime('%H:%M'),
                "Nubosidad aproximada (%)": round(cloud_score, 2)
            })

            break

        if best_image is None:
            st.warning(f"No se encontró ninguna imagen útil para la fecha {selected_date}")
            return None, None, None

        # 🔥 Usamos timestamp ya disponible (evita getInfo extra)
        image_date = datetime.utcfromtimestamp(selected_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

        # -------------------------------
        # Procesamiento de bandas
        # -------------------------------

        scl = best_image.select('SCL')
        cloud_mask = scl.neq(8).And(scl.neq(9)).And(scl.neq(10))

        bandas_requeridas = ['B2', 'B3', 'B4', 'B5', 'B6', 'B8A']

        clipped_image = best_image.clip(aoi)
        optical_bands = clipped_image.select(bandas_requeridas).divide(10000)
        scaled_image = clipped_image.addBands(optical_bands, overwrite=True)

        b3 = scaled_image.select('B3')
        b4 = scaled_image.select('B4')
        b5 = scaled_image.select('B5')
        b6 = scaled_image.select('B6')
        b8A = scaled_image.select('B8A')

        # -------------------------------
        # Índices
        # -------------------------------

        indices_functions = {

            "MCI": lambda: (
                b5.subtract(b4)
                  .subtract((b6.subtract(b4).multiply(40).divide(75)))
                  .updateMask(cloud_mask)
                  .rename('MCI')
            ),

            "PCI_B5/B4": lambda: (
                b5.divide(b4)
                  .updateMask(cloud_mask)
                  .rename('PCI_B5/B4')
            ),

            "NDCI_ind": lambda: (
                b5.subtract(b4)
                  .divide(b5.add(b4))
                  .updateMask(cloud_mask)
                  .rename('NDCI_ind')
            ),

            "PC_Val_cal": lambda: (
                ee.Image(100)
                .divide(
                    ee.Image(1).add(
                        (b5.divide(b4).subtract(1.9895)).multiply(-4.6755).exp()
                    )
                )
                .max(0)
                .updateMask(cloud_mask)
                .rename("PC_Val_cal")
            ),

            "Chla_Val_cal": lambda: (
                ee.Image(450)
                .divide(
                    ee.Image(1).add(
                        (b5.subtract(b4).divide(b5.add(b4)).subtract(0.46))
                        .multiply(-7.14)
                        .exp()
                    )
                )
                .max(0)
                .updateMask(cloud_mask)
                .rename("Chla_Val_cal")
            ),

            "PC_Bellus_cal": lambda: (
                ee.Image(16957)
                .multiply(
                    b6.subtract(
                        b8A.multiply(0.96).add(
                            (b3.subtract(b8A)).multiply(0.51)
                        )
                    )
                )
                .add(571)
                .max(0)
                .updateMask(cloud_mask)
                .rename("PC_Bellus_cal")
            ),

            "Chla_Bellus_cal": lambda: (
                ee.Image(112.78)
                .multiply(
                    b5.subtract(b4).divide(b5.add(b4))
                )
                .add(10.779)
                .max(0)
                .updateMask(cloud_mask)
                .rename("Chla_Bellus_cal")
            ),

            "UV_PC_Gral_cal": lambda: (
                ee.Image(24.665)
                .multiply(
                    b5.divide(b4).pow(3.4607)
                )
                .max(0)
                .updateMask(cloud_mask)
                .rename("UV_PC_Gral_cal")
            )
        }

        indices_to_add = []

        for index in selected_indices:
            if index in indices_functions:
                try:
                    indices_to_add.append(indices_functions[index]())
                except Exception as e:
                    st.warning(f"No se pudo calcular {index}: {e}")

        if not indices_to_add:
            return scaled_image, None, image_date

        indices_image = scaled_image.addBands(indices_to_add)

        return scaled_image, indices_image, image_date

def get_values_at_point(lat, lon, indices_image, selected_indices):
    if indices_image is None:
        return None

    # Sentinel-2 tiene 20 m de resolución para estas bandas -> 3x3 píxeles ~ 60x60 m
    buffer_radius_meters = 30  # Radio que cubre un área de 60x60 m
    point = ee.Geometry.Point([lon, lat]).buffer(buffer_radius_meters)

    values = {}
    for index in selected_indices:
        try:
            mean_value = indices_image.select(index).reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=point,
                scale=20,
                maxPixels=1e13
            ).get(index)
            values[index] = mean_value.getInfo() if mean_value is not None else None
        except Exception as e:
            print(f"⚠️ Error al obtener valor para {index} en punto ({lat}, {lon}): {e}")
            values[index] = None
    return values

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

def generar_url_geotiff_multibanda(indices_image, selected_indices, region, scale=20):
    try:
        url = indices_image.select(selected_indices).getDownloadURL({
            'scale': scale,
            'region': region.getInfo()['coordinates'],
            'fileFormat': 'GeoTIFF'
        })
        return url
    except Exception as e:
        return None

import pandas as pd
import requests
from io import StringIO
import streamlit as st

import pandas as pd
import requests
from io import StringIO
import streamlit as st
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

@st.cache_data(show_spinner=False)
def cargar_ficocianina_saica(estacion: int, fecha_ini: str, fecha_fin: str) -> pd.DataFrame:
    """
    Descarga los datos de ficocianina desde SAICA (CHE Ebro)
    con sistema de reintentos y tolerancia a fallos de conexión.
    """

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0 Safari/537.36"
        ),
        "Referer": f"https://saica.chebro.es/ficha.php?estacion={estacion}"
    }

    base_urls = [
        f"https://saica.chebro.es/fichaDataTabla.php?estacion={estacion}&fini={fecha_ini}&ffin={fecha_fin}",
        f"http://saica.chebro.es/fichaDataTabla.php?estacion={estacion}&fini={fecha_ini}&ffin={fecha_fin}"
    ]

    # Configurar reintentos
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    for url in base_urls:
        try:
            with st.spinner(f"📡 Descargando datos de SAICA ({fecha_ini} → {fecha_fin})..."):
                resp = session.get(url, headers=headers, timeout=60)
                resp.raise_for_status()

                # Si no devuelve HTML válido
                if "<html" not in resp.text.lower():
                    st.warning("⚠️ Respuesta inesperada del servidor SAICA.")
                    continue

                tablas = pd.read_html(StringIO(resp.text), header=None, flavor='lxml')
                if not tablas or len(tablas[0]) == 0:
                    st.warning("⚠️ No se encontró ninguna tabla con datos en la página de SAICA.")
                    continue

                df = tablas[0].iloc[:, :3]
                df.columns = ["Fecha-hora", "Ficocianina (µg/L)", "Temperatura (°C)"]

                df = df.dropna(subset=["Fecha-hora"])
                df["Fecha-hora"] = pd.to_datetime(df["Fecha-hora"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                df["Ficocianina (µg/L)"] = pd.to_numeric(df["Ficocianina (µg/L)"].astype(str).str.replace(",", "."), errors="coerce")
                df["Temperatura (°C)"] = pd.to_numeric(df["Temperatura (°C)"].astype(str).str.replace(",", "."), errors="coerce")

                df = df.dropna(subset=["Fecha-hora"])
                df = df.sort_values("Fecha-hora")

                if not df.empty:
                    return df

        except requests.exceptions.Timeout:
            st.warning(f"⚠️ Tiempo de espera agotado al conectar con SAICA ({url}). Reintentando...")
        except Exception as e:
            st.warning(f"⚠️ Error durante la conexión a SAICA ({url}): {e}")

    st.error("❌ No se pudo obtener datos de SAICA tras varios intentos.")
    return pd.DataFrame()


# INTERFAZ DE STREAMLIT

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600&family=Raleway:wght@600;700&display=swap');

    /* Fondo beige solo detrás de todo */
    .stApp {
        background-color: #e8e3d9 !important;
    }

    /* Tipografía global en todo el contenido */
    html, body, .stApp, p, label, span, h4, h5, h6, li, ul, ol, th, td, div, button, input, textarea, select, .css-1cpxqw2, .css-ffhzg2 {
        font-family: 'Playfair Display', serif !important;
        color: #000000;
    }

    /* TITULARES en "Aquatico" (simulado con Raleway) */
    h1, h2, h3, .main-title, .important-header {
        font-family: 'Raleway', sans-serif !important;
        color: #475a23;
        text-transform: uppercase;
        font-weight: 700;
    }

    /* Evitamos que ciertos elementos pierdan sus estilos */
    .stAlert, .stFileUploader, .stDataFrame, .stTable, .stExpander {
        background-color: white !important;
        color: black !important;
        border-radius: 8px;
    }
    
    /* Fondo transparente para sliders */
    .stSlider > div {
        background-color: transparent !important;
        box-shadow: none !important;
        border: none !important;
    }


    /* Tabs */
    div[role="tablist"] {
        font-family: 'Playfair Display', serif;
        display: flex;
        justify-content: center;
        font-size: 18px;
        font-weight: bold;
    }

    button[role="tab"] {
        background-color: #92c3ea33 !important;
        color: #475a23 !important;
        padding: 10px 25px;
        margin: 2px;
        border-radius: 0.5rem;
        font-size: 18px;
        font-family: 'Playfair Display', serif;
        border: 2px solid #92c3ea;
    }

    button[role="tab"][aria-selected="true"] {
        background-color: #5297d2 !important;
        color: white !important;
        border-color: #5297d2 !important;
    }

    /* Botones */
    .stButton>button {
        background-color: #5297d2;
        color: white;
        font-family: 'Playfair Display', serif;
        padding: 10px 20px;
        border-radius: 8px;
        border: none;
        font-size: 16px;
    }

    .stButton>button:hover {
        background-color: #92c3ea;
        color: black;
    }

    /* Cuadros informativos personalizados */
    .info-box {
        background-color: white;
        border-left: 6px solid #babf0a;
        padding: 15px;
        margin-bottom: 20px;
        border-radius: 8px;
        font-family: 'Playfair Display', serif;
    }

    /* Enlaces */
    a {
        color: #5297d2 !important;
        text-decoration: none;
    }

    a:hover {
        text-decoration: underline;
    }
    
    /* Fondo beige completo para los textos encima de los inputs */
.stSelectbox label, 
.stNumberInput label, 
.stTextInput label, 
.stSlider label {
    background-color: #e8e3d9 !important;
    padding: 0.3rem 0.7rem;
    border-radius: 0.3rem;
    display: block;
    width: 100%;
}
    /* Elimina fondo blanco en contenedores del botón */
section[data-testid="stButton"] {
    background-color: transparent !important;
}

/* Forzar fondo beige en toda la app excepto en widgets */
div.block-container {
    background-color: #eee7dc !important;  /* Marrón claro */
}

/* Evita que botones, selects y otros elementos tengan fondo blanco lateral */
.stButton, .stSelectbox, .stTextInput, .stNumberInput, .stDateInput {
    background-color: transparent !important;
}

/* Fondo para el texto de los widgets como file_uploader */
label[data-testid="stWidgetLabel"] > div {
    background-color: #e8e3d9 !important;
    padding: 0.5rem 0.8rem;
    border-radius: 0.5rem;
    display: inline-block;
}

/* Forzar fondo beige del contenedor completo del file_uploader */
div[data-testid="stFileUploader"] {
    background-color: #e8e3d9 !important;
    padding: 0.5rem;
    border-radius: 0.5rem;
}

/* También aplicarlo al dropzone interno */
div[data-testid="stFileUploaderDropzone"] {
    background-color: #e8e3d9 !important;
    border: none !important;
    border-radius: 0.5rem;
}

div[data-testid="element-container"] {
    box-shadow: none !important;
    padding: 0 !important;
    margin: 0 !important;
    background-color: transparent !important;
}



</style>

""", unsafe_allow_html=True)


col1, col2, col3 = st.columns([1, 4, 1.25])

with col1:
    st.image("images/logo_hiblooms.png", width=280)
    st.image("images/ministerio.png", width=280)

with col2:
    st.markdown(
        f"""
        <h1 style="text-align: center; line-height: 1.1em; font-size: 32px; margin: 0.3em 0;">
            {t("hero.l1")}
            <br><span style="display: block; text-align: center;">{t("hero.l2")}</span>
        </h1>
        """,
        unsafe_allow_html=True
    )

with col3:
    st.image("images/logo_bioma.png", width=320)
    col3a, col3b = st.columns([1, 1])
    with col3a:
        st.image("images/logo_ebro.png", width=130)
    with col3b:
        st.image("images/logo_jucar.png", width=170)

    # === Selector de idioma ===
    st.write("")  # pequeño respiro
    chosen = st.selectbox(
        t("ui.language"),
        options=_LANGS,
        format_func=lambda x: "Español" if x=="es" else "English",
        index=_LANGS.index(lang()),
    )
    if chosen != lang():
        set_lang(chosen)



tab1, tab2, tab3, tab4, tab5 = st.tabs([t("tabs.intro"), "Calibration", t("tabs.map"), t("tabs.tables"), t("tabs.quick")])
with tab1:
    # ==== Estilos: los dejas tal cual ====
    st.markdown("""
        <style>
            .header {
                font-size: 24px;
                font-weight: bold;
                text-align: center;
                padding: 10px;
                background-color: #1f77b4;
                color: white;
                border-radius: 10px;
                margin-bottom: 20px;
            }
            .info-box {
                padding: 15px;
                border-radius: 10px;
                background-color: #f4f4f4;
                margin-bottom: 15px;
            }
            .highlight {
                font-weight: bold;
                color: #1f77b4;
            }
        </style>
    """, unsafe_allow_html=True)

    # ==== Contenido bilingüe con el MISMO formato ====
    if lang() == "es":
        st.markdown(
            '<div class="header">Reconstrucción histórica y estado actual de la proliferación de cianobacterias en embalses españoles: HIBLOOMS (PID2023-153234OB-I00) </div>',
            unsafe_allow_html=True)

        st.markdown(
            '<div class="info-box"><b>Alineación con estrategias nacionales:</b><br>📌 Plan Nacional de Adaptación al Cambio Climático (PNACC 2021-2030)<br>📌 Directiva Marco del Agua 2000/60/EC<br>📌 Objetivo de Desarrollo Sostenible 6: Agua limpia y saneamiento</div>',
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
            <div class="info-box">
                <b>Equipo de Investigación:</b><br>
                🔬 <b>David Elustondo (DEV)</b> - BIOMA/UNAV, calidad del agua, QA/QC y biogeoquímica.<br>
                🔬 <b>Yasser Morera Gómez (YMG)</b> - BIOMA/UNAV, geoquímica isotópica y geocronología con <sup>210</sup>Pb.<br>
                🔬 <b>Esther Lasheras Adot (ELA)</b> - BIOMA/UNAV, técnicas analíticas y calidad del agua.<br>
                🔬 <b>Jesús Miguel Santamaría (JSU)</b> - BIOMA/UNAV, calidad del agua y técnicas analíticas.<br>
                🔬 <b>Carolina Santamaría Elola (CSE)</b> - BIOMA/UNAV, técnicas analíticas y calidad del agua.<br>
                🔬 <b>Adriana Rodríguez Garraus (ARG)</b> - MITOX/UNAV, análisis toxicológico.<br>
                🔬 <b>Sheila Izquieta Rojano (SIR)</b> - BIOMA/UNAV, SIG y teledetección, datos FAIR, digitalización.<br>
            </div>

            <div class="info-box">
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
            '<div class="header">Historical Reconstruction and Current Status of Cyanobacterial Blooms in Spanish Reservoirs (HIBLOOMS)</div>',
            unsafe_allow_html=True)

        st.markdown(
            '<div class="info-box"><b>Alignment with National Strategies:</b><br>📌 National Climate Change Adaptation Plan (PNACC 2021–2030)<br>📌 EU Water Framework Directive 2000/60/EC<br>📌 Sustainable Development Goal 6: Clean Water and Sanitation</div>',
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
            <div class="info-box">
                <b>Research Team:</b><br>
                🔬 <b>David Elustondo (DEV)</b> – BIOMA/UNAV, water quality, QA/QC, biogeochemistry.<br>
                🔬 <b>Yasser Morera Gómez (YMG)</b> – BIOMA/UNAV, isotopic geochemistry and <sup>210</sup>Pb geochronology.<br>
                🔬 <b>Esther Lasheras Adot (ELA)</b> – BIOMA/UNAV, analytical techniques, water quality.<br>
                🔬 <b>Jesús Miguel Santamaría (JSU)</b> – BIOMA/UNAV, water quality, analytical techniques.<br>
                🔬 <b>Carolina Santamaría Elola (CSE)</b> – BIOMA/UNAV, analytical techniques, water quality.<br>
                🔬 <b>Adriana Rodríguez Garraus (ARG)</b> – MITOX/UNAV, toxicological analysis.<br>
                🔬 <b>Sheila Izquieta Rojano (SIR)</b> – BIOMA/UNAV, GIS & remote sensing, FAIR data, digitalization.<br>
            </div>

            <div class="info-box">
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
    render_calibration_tab(obtener_nombres_embalses, load_reservoir_shapefile, gdf_to_ee_geometry)

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
    st.markdown("<hr style='border: 1px solid #b4a89b; margin: 2rem 0;'>", unsafe_allow_html=True)
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
    st.markdown("<hr style='border: 1px solid #b4a89b; margin: 2rem 0;'>", unsafe_allow_html=True)

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
    # Se muestra siempre que haya un job en curso (sin bloquear la UI)
    if "viz_job_id" in st.session_state and "viz_job_results" not in st.session_state:
        if _AUTOREFRESH_AVAILABLE:
            _st_autorefresh(interval=5000, key="viz_job_poller")

        _job_id = st.session_state["viz_job_id"]
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
            if not _AUTOREFRESH_AVAILABLE:
                st.info("Recarga la página para actualizar el estado del cálculo.")

        elif _state == "done":
            st.session_state["viz_job_results"] = _status.get("results", {})
            # Reconstruir session_state que usaban los widgets de resultados
            _res = st.session_state["viz_job_results"]
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
            df_available = pd.DataFrame(_fechas_unicas, columns=["Fecha"])
            df_available["Fecha"] = pd.to_datetime(df_available["Fecha"])
            df_available["Fecha_str"] = df_available["Fecha"].dt.strftime("%d %b")
            df_available["y_base"] = 0

            # Puntos en la línea de tiempo
            points = alt.Chart(df_available).mark_point(
                size=120, filled=True, opacity=0.85
            ).encode(
                x=alt.X("Fecha:T", title=None, axis=alt.Axis(
                    labelAngle=0, format="%d %b", tickCount=len(_fechas_unicas)
                )),
                y=alt.Y("y_base:Q", axis=None),
                tooltip=[alt.Tooltip("Fecha:T", title="Fecha", format="%d-%m-%Y")],
                color=alt.value("#5297d2"),
            )
            # Etiquetas encima de cada punto
            labels = alt.Chart(df_available).mark_text(
                align="center", baseline="bottom", dy=-14, fontSize=12, fontWeight=500
            ).encode(
                x="Fecha:T",
                y=alt.value(50),
                text="Fecha_str:N",
                color=alt.value("#475a23"),
            )
            # Línea base que conecta los puntos
            line = alt.Chart(df_available).mark_line(
                strokeWidth=1.5, opacity=0.3
            ).encode(
                x="Fecha:T",
                y="y_base:Q",
                color=alt.value("#5297d2"),
            )
            st.altair_chart(
                (line + points + labels)
                .properties(height=100, width="container")
                .configure_axis(labelFontSize=12)
                .configure_view(strokeWidth=0),
                use_container_width=True,
            )

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
                                            _, indices_image, _ = process_sentinel2(aoi, day, max_cloud_percentage, selected_indices)
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
