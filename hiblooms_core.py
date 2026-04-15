# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 11:43:42 2025

@author: ngoyenaserv
"""

# encoding: utf-8
"""
HIBLOOMS - Núcleo de lógica y procesamiento (sin UI)

Requisitos:
    pip install earthengine-api geopandas pandas numpy shapely

Resumen:
- init_ee(): inicializa Google Earth Engine (con o sin cuenta de servicio).
- obtención y carga de embalses desde shapefile -> ee.Geometry.
- cálculo de nubosidad/cobertura (S2 SR HARMONIZED).
- construcción de índices (MCI, NDCI_ind, PCI_B5/B4, PC_Val_cal, Chla_Val_cal,
  PC_Bellus_cal, Chla_Bellus_cal, UV_PC_Gral_cal).
- medias diarias del embalse (solo píxeles de agua SCL==6; en 2018 también SCL==2).
- valores medios en puntos de interés (buffer 30 m).
- distribución de área por clases (bins).
- generación de URL de descarga GeoTIFF multibanda.
- procesamiento por lote (varias fechas) -> resultados listos para workflow.

Ejemplo de uso mínimo:
    import hiblooms_core as hb
    hb.init_ee()

    gdf = hb.load_reservoir_shapefile("El Val", "shapefiles/embalses_hiblooms.shp")
    aoi = hb.gdf_to_ee_geometry(gdf)
    dates = hb.get_available_dates(aoi, "2024-06-01", "2024-07-01", max_cloud_percentage=60)

    out = hb.run_batch_processing(
        aoi=aoi,
        available_dates=dates,
        selected_indices=("PC_Val_cal","Chla_Val_cal"),
        max_cloud_percentage=60,
        puntos_interes={"El Val":{"Sonda":(41.8761,-1.7883)}},
    )
"""
import os
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union

from datetime import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import ee


# ──────────────────────────────────────────────────────────────────────────────
# Inicialización de Google Earth Engine
# ──────────────────────────────────────────────────────────────────────────────

def init_ee(service_account_json: Optional[Union[str, dict]] = None) -> None:
    """
    Inicializa Google Earth Engine.
    - service_account_json: dict o string JSON (o ruta a archivo JSON). Si None, usa credenciales locales.
    """
    try:
        if service_account_json is None:
            ee.Initialize()
            return

        if isinstance(service_account_json, dict):
            json_obj = service_account_json
        else:
            # Puede ser ruta a archivo o un string JSON
            if os.path.exists(str(service_account_json)):
                with open(str(service_account_json), "r", encoding="utf-8") as f:
                    json_obj = json.load(f)
            else:
                json_obj = json.loads(str(service_account_json))

        credentials = ee.ServiceAccountCredentials(json_obj["client_email"], key_data=json.dumps(json_obj))
        ee.Initialize(credentials)
    except Exception as e:
        raise RuntimeError(f"No se pudo inicializar Google Earth Engine: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Utilidades de shapefile y geometrías
# ──────────────────────────────────────────────────────────────────────────────

def obtener_nombres_embalses(shapefile_path: str = "shapefiles/embalses_hiblooms.shp") -> List[str]:
    """Devuelve la lista ordenada de nombres de embalses (columna 'NOMBRE')."""
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"No se encontró el archivo {shapefile_path}.")
    gdf = gpd.read_file(shapefile_path)
    if "NOMBRE" not in gdf.columns:
        raise ValueError("El shapefile no contiene la columna 'NOMBRE'.")
    return sorted(gdf["NOMBRE"].dropna().unique().tolist())

def load_reservoir_shapefile(
    reservoir_name: str,
    shapefile_path: str = "shapefiles/embalses_hiblooms.shp"
) -> gpd.GeoDataFrame:
    """Carga y filtra un embalse por nombre; reproyecta a EPSG:4326 si es necesario."""
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(f"No se encontró el archivo {shapefile_path}.")
    gdf = gpd.read_file(shapefile_path)
    if "NOMBRE" not in gdf.columns:
        raise ValueError("El shapefile no contiene la columna 'NOMBRE'.")

    if gdf.crs is None or (gdf.crs.to_epsg() or 0) != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Normaliza nombre para robustez
    gdf["_N_"] = gdf["NOMBRE"].str.lower().str.replace(" ", "_", regex=False)
    key = reservoir_name.lower().replace(" ", "_")
    gdf_f = gdf[gdf["_N_"] == key].copy()
    if gdf_f.empty:
        raise ValueError(f"No se encontró el embalse '{reservoir_name}' en el shapefile.")
    return gdf_f

def gdf_to_ee_geometry(gdf: gpd.GeoDataFrame) -> ee.Geometry:
    """Convierte el primer polígono del GeoDataFrame (EPSG:4326) a ee.Geometry.Polygon."""
    if gdf.empty:
        raise ValueError("El GeoDataFrame está vacío.")
    if gdf.crs is None or (gdf.crs.to_epsg() or 0) != 4326:
        raise ValueError("El GeoDataFrame debe estar en EPSG:4326.")
    geom = gdf.geometry.iloc[0]
    if geom.geom_type == "MultiPolygon":
        geom = list(geom.geoms)[0]
    coords = list(geom.exterior.coords)
    return ee.Geometry.Polygon([coords], geodesic=False)


# ──────────────────────────────────────────────────────────────────────────────
# Cálculos de nubosidad y cobertura
# ──────────────────────────────────────────────────────────────────────────────

def calculate_cloud_percentage(image: ee.Image, aoi: ee.Geometry) -> Optional[ee.Number]:
    """
    Calcula % de nubosidad combinando SCL (7,8,9,10) y MSK_CLDPRB (>=10).
    Excluye vegetación (4) y suelo desnudo (5).
    Devuelve ee.Number (0-100) o None si no es posible.
    """
    scl = image.select("SCL")
    cloud_mask_scl = scl.eq(7).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
    non_valid_mask = scl.eq(4).Or(scl.eq(5))
    valid_pixels_mask = scl.mask().And(non_valid_mask.Not())

    cloud_fraction_scl = cloud_mask_scl.updateMask(valid_pixels_mask).reduceRegion(
        reducer=ee.Reducer.mean(), geometry=aoi, scale=20, maxPixels=1e13
    ).get("SCL")

    cloud_mask_prob = image.select("MSK_CLDPRB").gte(10).updateMask(valid_pixels_mask)
    cloud_fraction_prob = cloud_mask_prob.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=aoi, scale=20, maxPixels=1e13
    ).get("MSK_CLDPRB")

    scl_ok = cloud_fraction_scl is not None
    prob_ok = cloud_fraction_prob is not None

    if not scl_ok and not prob_ok:
        return None

    if scl_ok and prob_ok:
        return (
            ee.Number(cloud_fraction_scl).multiply(0.95)
            .add(ee.Number(cloud_fraction_prob).multiply(0.05))
            .multiply(100)
        )
    if scl_ok:
        return ee.Number(cloud_fraction_scl).multiply(100)
    return ee.Number(cloud_fraction_prob).multiply(100)

def calculate_coverage_percentage(image: ee.Image, aoi: ee.Geometry) -> float:
    """Devuelve el % de cobertura válida (por máscara de B4) sobre el AOI."""
    try:
        total_pixels = ee.Image(1).clip(aoi).reduceRegion(
            reducer=ee.Reducer.count(), geometry=aoi, scale=20, maxPixels=1e13
        ).get("constant")
        valid_mask = image.select("B4").mask()
        valid_pixels = ee.Image(1).updateMask(valid_mask).clip(aoi).reduceRegion(
            reducer=ee.Reducer.count(), geometry=aoi, scale=20, maxPixels=1e13
        ).get("constant")
        if total_pixels is None or valid_pixels is None:
            return 0.0
        coverage = ee.Number(valid_pixels).divide(ee.Number(total_pixels)).multiply(100)
        return float(coverage.getInfo())
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Fechas disponibles (filtradas por nubosidad/cobertura)
# ──────────────────────────────────────────────────────────────────────────────

def get_available_dates(
    aoi: ee.Geometry,
    start_date: str,
    end_date: str,
    max_cloud_percentage: int,
    min_coverage_percentage: float = 50.0,
) -> List[str]:
    """
    Devuelve fechas (YYYY-MM-DD) con imágenes S2_SR_HARMONIZED dentro del rango (UTC),
    filtradas por nubosidad estimada y cobertura mínima.
    """
    sentinel2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(aoi)
        .filterDate(start_date, end_date)
    )

    if sentinel2.size().getInfo() == 0:
        return []

    images = sentinel2.toList(sentinel2.size())
    results: List[str] = []
    seen = set()

    for i in range(int(images.size().getInfo())):
        img = ee.Image(images.get(i)).clip(aoi)
        t0 = img.get("system:time_start").getInfo()
        date_str = datetime.utcfromtimestamp(t0 / 1000).strftime("%Y-%m-%d")

        if date_str in seen:
            continue

        cloud_obj = calculate_cloud_percentage(img, aoi)
        if cloud_obj is None:
            continue
        try:
            cloud_pct = float(ee.Number(cloud_obj).getInfo())
        except Exception:
            continue

        coverage = calculate_coverage_percentage(img, aoi)
        if (max_cloud_percentage == 100 or cloud_pct <= max_cloud_percentage) and coverage >= float(min_coverage_percentage):
            results.append(date_str)
            seen.add(date_str)

    return sorted(results)


# ──────────────────────────────────────────────────────────────────────────────
# Cálculo de índices e imágenes
# ──────────────────────────────────────────────────────────────────────────────

def _ensure_bands_available(image: ee.Image, bands: Sequence[str], date_label: str = "") -> None:
    available = image.bandNames().getInfo()
    missing = [b for b in bands if b not in available]
    if missing:
        raise ValueError(f"Faltan bandas {missing} en la imagen {date_label or ''}.")

def _build_indices_image(
    base_image: ee.Image,
    aoi: ee.Geometry,
    selected_indices: Sequence[str]
) -> ee.Image:
    """
    Construye una imagen con las bandas base + índices seleccionados.
    Aplica máscara de nubes (SCL != 8,9,10) a los índices.
    """
    scl = base_image.select("SCL")
    cloud_mask = scl.neq(8).And(scl.neq(9)).And(scl.neq(10))

    required = ["B2", "B3", "B4", "B5", "B6", "B8A"]
    _ensure_bands_available(base_image, required)

    clipped = base_image.clip(aoi)
    optical = clipped.select(required).divide(10000)
    scaled = clipped.addBands(optical, overwrite=True)

    b3 = scaled.select("B3")
    b4 = scaled.select("B4")
    b5 = scaled.select("B5")
    b6 = scaled.select("B6")
    b8A = scaled.select("B8A")

    # Definición de índices (idéntica a tu app)
    indices_functions = {
        "MCI": lambda: b5.subtract(b4).subtract((b6.subtract(b4).multiply(705 - 665).divide(740 - 665))).updateMask(cloud_mask).rename("MCI"),
        "PCI_B5/B4": lambda: b5.divide(b4).updateMask(cloud_mask).rename("PCI_B5/B4"),
        "NDCI_ind": lambda: b5.subtract(b4).divide(b5.add(b4)).updateMask(cloud_mask).rename("NDCI_ind"),
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
        ),
    }

    bands_to_add = []
    for idx in selected_indices:
        if idx in indices_functions:
            bands_to_add.append(indices_functions[idx]())
        else:
            # Silencioso: si un índice no existe, no se añade.
            pass

    if not bands_to_add:
        return scaled  # sin índices adicionales
    return scaled.addBands(bands_to_add)


def process_sentinel2(
    aoi: ee.Geometry,
    selected_date: str,
    max_cloud_percentage: int,
    selected_indices: Sequence[str],
    min_coverage_percentage: float = 50.0,
) -> Tuple[Optional[ee.Image], Optional[ee.Image], Optional[str], Optional[float], Optional[float]]:
    """
    Devuelve:
      (scaled_image, indices_image, image_datetime_iso, cloud_pct, coverage_pct)
    para la mejor imagen disponible en la fecha (UTC), cumpliendo umbrales.
    """
    date_ee = ee.Date(selected_date)
    end_ee = date_ee.advance(1, "day")

    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(aoi)
        .filterDate(date_ee, end_ee)
    )
    n = s2.size().getInfo()
    if n == 0:
        return None, None, None, None, None

    images = s2.toList(n)

    # Seleccionar la imagen con menor nubosidad que cumpla cobertura/nubosidad
    best = None
    best_cloud = None
    best_cov = None

    for i in range(n):
        img = ee.Image(images.get(i))
        cloud_obj = calculate_cloud_percentage(img, aoi)
        if cloud_obj is None:
            continue
        try:
            cloud_pct = float(ee.Number(cloud_obj).getInfo())
        except Exception:
            continue

        cov = calculate_coverage_percentage(img, aoi)

        if (max_cloud_percentage == 100 or cloud_pct <= max_cloud_percentage) and cov >= float(min_coverage_percentage):
            if (best_cloud is None) or (cloud_pct < best_cloud):
                best = img
                best_cloud = cloud_pct
                best_cov = cov

    if best is None:
        return None, None, None, None, None

    t0 = best.get("system:time_start").getInfo()
    image_iso = datetime.utcfromtimestamp(t0 / 1000).strftime("%Y-%m-%d %H:%M:%S")

    # Escalar bandas ópticas de DN (0-10000) a reflectancia (0-1)
    # igual que hacía app.py originalmente
    bandas_requeridas = ["B2", "B3", "B4", "B5", "B6", "B8A"]
    clipped = best.clip(aoi)
    optical = clipped.select(bandas_requeridas).divide(10000)
    scaled = clipped.addBands(optical, overwrite=True)

    indices_image = _build_indices_image(scaled, aoi, selected_indices)

    return scaled, indices_image, image_iso, best_cloud, best_cov


# ──────────────────────────────────────────────────────────────────────────────
# Medias diarias, valores en punto y distribución por clases
# ──────────────────────────────────────────────────────────────────────────────

def calcular_media_diaria_embalse(indices_image: ee.Image, index_name: str, aoi: ee.Geometry) -> Optional[float]:
    """
    Media del índice en el embalse solo en SCL==6 (o SCL==2 también para 2018).
    """
    scl = indices_image.select("SCL")
    # Extraer fecha
    millis = indices_image.get("system:time_start").getInfo()
    year = datetime.utcfromtimestamp(millis / 1000).year if millis is not None else None

    mask_agua = scl.eq(6) if year != 2018 else scl.eq(6).Or(scl.eq(2))
    ind = indices_image.select(index_name).updateMask(mask_agua)
    mean_val = ind.reduceRegion(
        reducer=ee.Reducer.mean(), geometry=aoi, scale=20, maxPixels=1e13
    ).get(index_name)
    try:
        return float(mean_val.getInfo()) if mean_val is not None else None
    except Exception:
        return None

def get_values_at_point(lat: float, lon: float, indices_image: ee.Image, selected_indices: Sequence[str]) -> Dict[str, Optional[float]]:
    """
    Valor medio en un buffer ~60x60 m (radio 30 m) sobre cada índice solicitado.
    """
    point = ee.Geometry.Point([lon, lat]).buffer(30)
    out: Dict[str, Optional[float]] = {}
    for idx in selected_indices:
        try:
            val = indices_image.select(idx).reduceRegion(
                reducer=ee.Reducer.mean(), geometry=point, scale=20, maxPixels=1e13
            ).get(idx)
            out[idx] = float(val.getInfo()) if val is not None else None
        except Exception:
            out[idx] = None
    return out

def calcular_distribucion_area_por_clases(
    indices_image: ee.Image,
    index_name: str,
    aoi: ee.Geometry,
    bins: Sequence[float]
) -> List[Dict[str, float]]:
    """
    Devuelve una lista de dicts con: {"rango": "low–up", "area_ha": ha, "porcentaje": %}
    calculada sobre píxeles de agua (regla SCL==6; en 2018 también SCL==2).
    """
    scl = indices_image.select("SCL")
    millis = indices_image.get("system:time_start").getInfo()
    year = datetime.utcfromtimestamp(millis / 1000).year if millis is not None else None

    mask_agua = scl.eq(6) if year != 2018 else scl.eq(6).Or(scl.eq(2))
    img_idx = indices_image.select(index_name).updateMask(mask_agua)
    pixel_area = ee.Image.pixelArea().updateMask(img_idx.mask())

    parts: List[Dict[str, float]] = []
    for i in range(len(bins) - 1):
        low, up = float(bins[i]), float(bins[i + 1])
        bin_mask = img_idx.gte(low).And(img_idx.lt(up))
        bin_area = pixel_area.updateMask(bin_mask).reduceRegion(
            reducer=ee.Reducer.sum(), geometry=aoi, scale=20, maxPixels=1e13
        ).get("area")
        parts.append({
            "rango": f"{low}–{up}",
            "area_ha": ee.Number(bin_area).divide(10000)  # m² → ha
        })

    total_area = pixel_area.reduceRegion(
        reducer=ee.Reducer.sum(), geometry=aoi, scale=20, maxPixels=1e13
    ).get("area")
    try:
        total_ha = float(ee.Number(total_area).divide(10000).getInfo())
    except Exception:
        total_ha = 0.0

    out: List[Dict[str, float]] = []
    for r in parts:
        try:
            area_ha = float(r["area_ha"].getInfo())
        except Exception:
            area_ha = 0.0
        pct = (area_ha / total_ha * 100.0) if total_ha > 0 else 0.0
        out.append({"rango": r["rango"], "area_ha": area_ha, "porcentaje": pct})
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Exportación GeoTIFF multibanda (URL de descarga)
# ──────────────────────────────────────────────────────────────────────────────

def generar_url_geotiff_multibanda(indices_image, selected_indices, region, scale=20):
    """
    Genera URL de descarga para un GeoTIFF multibanda,
    con nombres de bandas seguros (sin caracteres conflictivos).
    """
    try:
        # Nombres seguros (sin caracteres conflictivos)
        safe_names = [name.replace("/", "_") for name in selected_indices]

        # Seleccionar y renombrar al vuelo
        export_img = indices_image.select(list(selected_indices), safe_names)

        url = export_img.getDownloadURL({
            'scale': scale,
            'region': region.getInfo()['coordinates'],
            'fileFormat': 'GeoTIFF'
        })
        return url
    except Exception:
        return None



# ──────────────────────────────────────────────────────────────────────────────
# Orquestación por lote (varias fechas)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BatchResult:
    data_time: List[Dict]                  # series de puntos y medias de embalse
    used_cloud_results: List[Dict]         # nubosidad por imagen usada
    urls_exportacion: List[Dict]           # {"fecha": str, "url": str}
    processed_dates: List[str]             # fechas efectivamente procesadas

def run_batch_processing(
    aoi: ee.Geometry,
    available_dates: Sequence[str],
    selected_indices: Sequence[str],
    max_cloud_percentage: int = 60,
    min_coverage_percentage: float = 50.0,
    puntos_interes: Optional[Dict[str, Dict[str, Tuple[float, float]]]] = None,
    reservoir_name_for_pois: Optional[str] = None,
    compute_distributions: bool = False,
    distribution_bins_by_index: Optional[Dict[str, Sequence[float]]] = None
) -> BatchResult:
    """
    Procesa múltiples fechas y devuelve:
      - data_time: registros (puntos, medias) listos para tablas/series.
      - used_cloud_results: nubosidad/cobertura de cada imagen usada.
      - urls_exportacion: URLs de descarga GeoTIFF (multibanda en cada fecha).
      - processed_dates: fechas para las que se obtuvo imagen válida.

    Nota:
      - puntos_interes: dict opcional {reservoir_name: {point_name: (lat, lon)}}.
        Si se especifica, se usa el subdict por 'reservoir_name_for_pois'.
    """
    data_time: List[Dict] = []
    used_cloud_results: List[Dict] = []
    urls_exportacion: List[Dict] = []
    processed_dates: List[str] = []

    # Determina puntos de interés activos (si se pasan)
    pois: Dict[str, Tuple[float, float]] = {}
    if puntos_interes and reservoir_name_for_pois and (reservoir_name_for_pois in puntos_interes):
        pois = puntos_interes[reservoir_name_for_pois].copy()

    for day in available_dates:
        scaled, indices_img, image_iso, cloud_pct, cov_pct = process_sentinel2(
            aoi=aoi,
            selected_date=day,
            max_cloud_percentage=max_cloud_percentage,
            selected_indices=selected_indices,
            min_coverage_percentage=min_coverage_percentage,
        )
        if indices_img is None or image_iso is None:
            continue

        processed_dates.append(day)
        if cloud_pct is not None:
            used_cloud_results.append({
                "Fecha": day,
                "Hora": image_iso.split(" ")[1],
                "Nubosidad aproximada (%)": round(float(cloud_pct), 2),
                "Cobertura (%)": round(float(cov_pct or 0.0), 2),
            })

        # URL de exportación GeoTIFF multibanda
        url = generar_url_geotiff_multibanda(indices_img, selected_indices, aoi)
        if url:
            urls_exportacion.append({"fecha": day, "url": url})

        # Valores en puntos de interés
        if pois:
            for point_name, (lat, lon) in pois.items():
                vals = get_values_at_point(lat, lon, indices_img, selected_indices)
                reg = {"Point": point_name, "Date": day, "Tipo": "Valor Estimado"}
                for idx in selected_indices:
                    if vals.get(idx) is not None:
                        reg[idx] = vals[idx]
                if any(k in reg for k in selected_indices):
                    data_time.append(reg)

        # Medias del embalse (por índice)
        for idx in selected_indices:
            media = calcular_media_diaria_embalse(indices_img, idx, aoi)
            if media is not None:
                data_time.append({"Point": "Media_Embalse", "Date": day, idx: media, "Tipo": "Valor Estimado"})

        # Distribuciones por clases (opcional)
        if compute_distributions and distribution_bins_by_index:
            for idx, bins in distribution_bins_by_index.items():
                try:
                    dist = calcular_distribucion_area_por_clases(indices_img, idx, aoi, bins)
                    # Puedes capturar estas distribuciones aquí si las necesitas en salida;
                    # por simplicidad, se podrían adjuntar a data_time con etiquetas específicas:
                    for d in dist:
                        data_time.append({
                            "Point": "Distribucion_Embalse",
                            "Date": day,
                            "Indice": idx,
                            "Rango": d["rango"],
                            "Area_ha": d["area_ha"],
                            "Porcentaje": d["porcentaje"],
                            "Tipo": "Distribucion"
                        })
                except Exception:
                    pass

    return BatchResult(
        data_time=data_time,
        used_cloud_results=used_cloud_results,
        urls_exportacion=urls_exportacion,
        processed_dates=processed_dates,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Ejecución directa (demo CLI mínima)
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Demo ligero: ajusta rutas/nombres si lo quieres probar rápidamente.
    init_ee()  # Usa credenciales locales ya configuradas

    shp = "shapefiles/embalses_hiblooms.shp"
    name = "El Val"

    gdf = load_reservoir_shapefile(name, shp)
    aoi = gdf_to_ee_geometry(gdf)

    start, end = "2024-06-01", "2024-06-15"
    dates = get_available_dates(aoi, start, end, max_cloud_percentage=60)

    result = run_batch_processing(
        aoi=aoi,
        available_dates=dates,
        selected_indices=("MCI", "NDCI_ind", "PC_Val_cal"),
        max_cloud_percentage=60,
        puntos_interes={name: {"Sonda": (41.8761, -1.7883)}},
        reservoir_name_for_pois=name,
        compute_distributions=True,
        distribution_bins_by_index={
            "PC_Val_cal": np.linspace(0, 10, 6),
            "MCI": np.linspace(-0.1, 0.4, 6),
        },
    )

    # Muestra un resumen rápido por consola
    print("Fechas procesadas:", result.processed_dates)
    print("Nubosidad usada (n):", len(result.used_cloud_results))
    print("URLs exportación (n):", len(result.urls_exportacion))
    print("Registros data_time (n):", len(result.data_time))
