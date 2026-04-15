# api/worker.py
#
# Worker síncrono que ejecuta los workflows de HIBLOOMS.
# Es llamado por api/main.py como BackgroundTask.
#
# Requiere la variable de entorno:
#   GEE_SERVICE_ACCOUNT_JSON  →  JSON de la cuenta de servicio GEE
#                                (el mismo valor que en secrets.toml)

from __future__ import annotations

import base64
import io
import json
import logging
import os
from typing import Any, Callable, Dict, List

import ee
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import shape

log = logging.getLogger(__name__)

# Tipos de los callbacks que recibe cada worker desde main.py
ProgressFn = Callable[[str, str, int], None]   # (job_id, step, pct)
CompleteFn = Callable[[str, Dict], None]        # (job_id, results)
FailFn     = Callable[[str, str], None]         # (job_id, error_msg)


# ---------------------------------------------------------------------------
# Inicialización de GEE
# ---------------------------------------------------------------------------
def _init_ee() -> None:
    """
    Inicializa GEE usando la variable de entorno GEE_SERVICE_ACCOUNT_JSON.
    El valor es el mismo JSON que ya tienes en secrets.toml.
    """
    raw = os.environ.get("GEE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError(
            "Variable de entorno GEE_SERVICE_ACCOUNT_JSON no definida. "
            "Debe contener el JSON de la cuenta de servicio."
        )
    json_obj = json.loads(raw)
    credentials = ee.ServiceAccountCredentials(
        json_obj["client_email"], key_data=json.dumps(json_obj)
    )
    ee.Initialize(credentials)


# ---------------------------------------------------------------------------
# Helper: reconstruir GeoDataFrame y geometría EE desde GeoJSON string
# ---------------------------------------------------------------------------
def _aoi_from_geojson(aoi_geojson: str):
    gdf = gpd.read_file(io.StringIO(aoi_geojson))
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    geometry = gdf.geometry.iloc[0]
    if geometry.geom_type == "MultiPolygon":
        geometry = list(geometry.geoms)[0]
    coords = list(geometry.exterior.coords)
    return gdf, ee.Geometry.Polygon([coords], geodesic=False)


# ---------------------------------------------------------------------------
# WORKER: Visualización
# ---------------------------------------------------------------------------
def run_visualization_job(
    job_id: str,
    config: Dict[str, Any],
    update: ProgressFn,
    complete: CompleteFn,
    fail: FailFn,
) -> None:
    try:
        log.info(f"[{job_id}] Visualization job started")
        _init_ee()

        reservoir_name    = config["reservoir"]
        start_date        = config["start_date"]
        end_date          = config["end_date"]
        max_cloud_pct     = int(config["max_cloud_pct"])
        selected_indices  = config["indices"]
        aoi_geojson       = config["aoi_geojson"]
        puntos_interes_raw = config.get("puntos_interes", {})

        # puntos_interes: {"Sonda": [lat, lon], ...}
        puntos_interes = {k: tuple(v) for k, v in puntos_interes_raw.items()}

        # ── Paso 1: Reconstruir geometría ────────────────────────────────
        update(job_id, "Cargando geometría del embalse…", 5)
        gdf, aoi = _aoi_from_geojson(aoi_geojson)

        # ── Paso 2: Obtener fechas disponibles ───────────────────────────
        update(job_id, "Buscando imágenes Sentinel-2 disponibles…", 10)

        # Importar funciones de núcleo (sin UI)
        from hiblooms_core import get_available_dates, process_sentinel2  # type: ignore

        # Comprobar si hay CSV precalculado para val/bellus con nubosidad 60%
        cloud_results: List[Dict] = []
        available_dates: List[str] = []

        usar_csv = (
            reservoir_name.lower() in ("val", "bellus") and max_cloud_pct == 60
        )
        if usar_csv:
            csv_map = {
                "val":    "fechas_validas_el_val_historico.csv",
                "bellus": "fechas_validas_bellus_historico.csv",
            }
            csv_path = csv_map[reservoir_name.lower()]
            if os.path.exists(csv_path):
                df_fechas = pd.read_csv(csv_path)
                if "fecha" in df_fechas.columns:
                    df_fechas.rename(columns={"fecha": "Fecha"}, inplace=True)
                df_fechas["Fecha"] = pd.to_datetime(df_fechas["Fecha"], errors="coerce")
                df_fechas = df_fechas.dropna(subset=["Fecha"])
                mask = (
                    (df_fechas["Fecha"] >= pd.to_datetime(start_date)) &
                    (df_fechas["Fecha"] <= pd.to_datetime(end_date))
                )
                fechas_filtradas = df_fechas[mask]["Fecha"].dt.strftime("%Y-%m-%d").tolist()
                available_dates = sorted(fechas_filtradas)

                if available_dates and "nubosidad" in df_fechas.columns:
                    df_fechas = df_fechas.set_index("Fecha")
                    for f in available_dates:
                        try:
                            nub = df_fechas.loc[pd.to_datetime(f), "nubosidad"]
                        except Exception:
                            nub = None
                        cloud_results.append({
                            "Fecha": f,
                            "Hora": "00:00",
                            "Nubosidad aproximada (%)": round(float(nub), 2) if nub is not None else "Desconocida",
                            "Cobertura (%)": 100,
                        })

        if not available_dates:
            available_dates = get_available_dates(aoi, start_date, end_date, max_cloud_pct)

        if not available_dates:
            complete(job_id, {
                "available_dates": [],
                "data_time": [],
                "cloud_results": [],
                "used_cloud_results": [],
                "selected_indices": selected_indices,
                "urls_exportacion": [],
                "tile_urls": [],
            })
            return

        update(job_id, f"Fechas encontradas: {len(available_dates)}. Procesando imágenes…", 20)

        # ── Paso 3: Procesar cada fecha ──────────────────────────────────
        clorofila_indices   = {"MCI", "NDCI_ind", "Chla_Val_cal", "Chla_Bellus_cal"}
        ficocianina_indices = {"UV_PC_Gral_cal", "PC_Val_cal", "PCI_B5/B4", "PC_Bellus_cal"}
        hay_clorofila   = any(i in selected_indices for i in clorofila_indices)
        hay_ficocianina = any(i in selected_indices for i in ficocianina_indices)

        data_time: List[Dict] = []
        used_cloud_results: List[Dict] = []
        urls_exportacion: List[Dict] = []
        tile_urls: List[Dict] = []

        # Datos SAICA (El Val, ficocianina)
        if reservoir_name.lower() == "val" and hay_ficocianina:
            try:
                import requests as _req
                urls_csv = [
                    "https://drive.google.com/uc?id=1-FpLJpudQd69r9JxTbT1EhHG2swASEn-&export=download",
                    "https://drive.google.com/uc?id=1w5vvpt1TnKf_FN8HaM9ZVi3WSf0ibxlV&export=download",
                ]
                df_list = []
                for url in urls_csv:
                    try:
                        df_list.append(pd.read_csv(url))
                    except Exception:
                        pass
                df_list = [df for df in df_list if not df.empty]
                if df_list:
                    df_fico = pd.concat(df_list)
                    if "Time" in df_fico.columns:
                        df_fico.rename(columns={"Time": "Fecha-hora"}, inplace=True)
                    df_fico["Fecha-hora"] = pd.to_datetime(df_fico["Fecha-hora"], format="mixed")
                    df_fico = df_fico.sort_values("Fecha-hora")
                    mask = (
                        (df_fico["Fecha-hora"] >= pd.to_datetime(start_date)) &
                        (df_fico["Fecha-hora"] <= pd.to_datetime(end_date))
                    )
                    for _, row in df_fico[mask].iterrows():
                        data_time.append({
                            "Point": "SAICA_Val",
                            "Date": str(row["Fecha-hora"]),
                            "Ficocianina (µg/L)": row.get("Ficocianina (µg/L)"),
                            "Tipo": "Valor Real",
                        })
            except Exception as e:
                log.warning(f"[{job_id}] No se pudieron cargar datos SAICA: {e}")

        n = len(available_dates)
        for i, day in enumerate(available_dates):
            pct = 20 + int(75 * (i / n))
            update(job_id, f"Procesando imagen {i+1}/{n}: {day}…", pct)

            try:
                scaled_image, indices_image, image_date, _cloud_pct, _cov_pct = process_sentinel2(
                    aoi, day, max_cloud_pct, selected_indices
                )
                if _cloud_pct is not None:
                    used_cloud_results.append({
                        "Fecha": day,
                        "Hora": image_date[11:16] if image_date else "00:00",
                        "Nubosidad aproximada (%)": round(_cloud_pct, 2),
                    })
            except Exception as e:
                log.warning(f"[{job_id}] Error en {day}: {e}")
                continue

            if indices_image is None:
                continue

            # Tile URLs para los mapas Folium
            from hiblooms_core import generar_url_geotiff_multibanda  # type: ignore
            index_palettes = {
                "MCI":           ["blue", "green", "yellow", "red"],
                "PCI_B5/B4":     ["#ADD8E6", "#008000", "#FFFF00", "#FF0000"],
                "NDCI_ind":      ["blue", "green", "yellow", "red"],
                "UV_PC_Gral_cal":["#2171b5", "#c7e9c0", "#238b45", "#e31a1c"],
                "PC_Val_cal":    ["#ADD8E6", "#008000", "#FFFF00", "#FF0000"],
                "Chla_Val_cal":  ["#2171b5", "#c7e9c0", "#238b45", "#e31a1c"],
                "Chla_Bellus_cal":["#2171b5","#c7e9c0", "#238b45", "#e31a1c"],
                "PC_Bellus_cal": ["#2171b5", "#c7e9c0", "#238b45", "#e31a1c"],
            }
            vis_ranges = {
                "PC_Val_cal":     (0, 5),
                "PCI_B5/B4":      (0.5, 1.5),
                "Chla_Val_cal":   (0, 150),
                "Chla_Bellus_cal":(5, 100),
                "PC_Bellus_cal":  (25, 500),
                "UV_PC_Gral_cal": (0, 100),
            }

            try:
                from datetime import datetime as _dt
                image_date_fmt = _dt.strptime(image_date, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M")
                layers: Dict[str, str] = {}

                # RGB: igual que la app original — imagen completa sin clip adicional
                # scaled_image está clippeada al AOI (píxeles null fuera = negro)
                # Usamos unmask() para que los píxeles null sean transparentes
                layers["RGB"] = scaled_image.select(["B4", "B3", "B2"]).unmask().visualize(
                    bands=["B4", "B3", "B2"], min=0, max=0.3, gamma=1.4
                ).getMapId()["tile_fetcher"].url_format

                scl_colors = [
                    "#ff0004","#000000","#8B4513","#00FF00","#FFD700",
                    "#0000FF","#F4EEEC","#C8C2C0","#706C6B","#87CEFA","#00FFFF"
                ]
                layers["SCL"] = indices_image.select("SCL").visualize(
                    min=1, max=11, palette=scl_colors
                ).getMapId()["tile_fetcher"].url_format

                layers["Nubes"] = indices_image.select("MSK_CLDPRB").visualize(
                    min=0, max=100, palette=["blue","green","yellow","red","black"]
                ).getMapId()["tile_fetcher"].url_format

                for idx in selected_indices:
                    mn, mx = vis_ranges.get(idx, (-0.1, 0.4))
                    layers[idx] = indices_image.select(idx).visualize(
                        min=mn, max=mx, palette=index_palettes.get(idx, ["blue","green","yellow","red"])
                    ).getMapId()["tile_fetcher"].url_format

                tile_urls.append({"date": image_date_fmt, "layers": layers})
            except Exception as e:
                log.warning(f"[{job_id}] No se pudieron generar tile URLs para {day}: {e}")

            # Valores en puntos de interés
            try:
                from hiblooms_core import get_values_at_point  # type: ignore
                for point_name, (lat_point, lon_point) in puntos_interes.items():
                    values = get_values_at_point(lat_point, lon_point, indices_image, selected_indices)
                    registro = {"Point": point_name, "Date": day, "Tipo": "Valor Estimado"}
                    for indice in selected_indices:
                        if indice in values and values[indice] is not None:
                            registro[indice] = values[indice]
                    if any(k in registro for k in selected_indices):
                        data_time.append(registro)
            except Exception as e:
                log.warning(f"[{job_id}] Error puntos de interés {day}: {e}")

            # Media diaria del embalse
            try:
                from hiblooms_core import calcular_media_diaria_embalse  # type: ignore
                for idx in selected_indices:
                    if (hay_clorofila and idx in clorofila_indices) or \
                       (hay_ficocianina and idx in ficocianina_indices):
                        media = calcular_media_diaria_embalse(indices_image, idx, aoi)
                        if media is not None:
                            data_time.append({
                                "Point": "Media_Embalse",
                                "Date": day,
                                idx: media,
                                "Tipo": "Valor Estimado",
                            })
            except Exception as e:
                log.warning(f"[{job_id}] Error media diaria {day}: {e}")

            # URL de exportación GeoTIFF
            try:
                from hiblooms_core import generar_url_geotiff_multibanda  # type: ignore
                url = generar_url_geotiff_multibanda(indices_image, selected_indices, aoi)
                if url:
                    urls_exportacion.append({"fecha": day, "url": url})
            except Exception as e:
                log.warning(f"[{job_id}] Error GeoTIFF URL {day}: {e}")

        # ── Paso 4: Devolver resultados ──────────────────────────────────
        update(job_id, "Finalizando…", 98)

        results = {
            "available_dates":    available_dates,
            "selected_indices":   selected_indices,
            "data_time":          data_time,
            "cloud_results":      cloud_results,
            "used_cloud_results": used_cloud_results,
            "urls_exportacion":   urls_exportacion,
            "tile_urls":          tile_urls,
        }
        complete(job_id, results)
        log.info(f"[{job_id}] Visualization job completed")

    except Exception as e:
        log.error(f"[{job_id}] Visualization job failed: {e}", exc_info=True)
        fail(job_id, str(e))


# ---------------------------------------------------------------------------
# WORKER: Calibración
# ---------------------------------------------------------------------------
def run_calibration_job(
    job_id: str,
    config: Dict[str, Any],
    update: ProgressFn,
    complete: CompleteFn,
    fail: FailFn,
) -> None:
    try:
        log.info(f"[{job_id}] Calibration job started")
        _init_ee()

        reservoir_name     = config["reservoir"]
        target_variable    = config["target_variable"]
        start_hour         = int(config["start_hour"])
        end_hour           = int(config["end_hour"])
        overpass_window    = float(config["overpass_window"])
        max_cloud          = int(config["max_cloud"])
        min_coverage       = int(config["min_coverage"])
        predictor_set      = config["predictor_set"]
        selected_models    = config["selected_models"]
        cv_scheme          = config["cv_scheme"]
        outlier_method     = config["outlier_method"]
        min_samples        = int(config["min_samples_required"])
        aoi_geojson        = config["aoi_geojson"]
        insitu_csv_b64     = config["insitu_csv_b64"]

        # ── Paso 1: Decodificar CSV in-situ ─────────────────────────────
        update(job_id, "Cargando datos in-situ…", 5)
        csv_bytes = base64.b64decode(insitu_csv_b64)
        df_raw = pd.read_csv(io.BytesIO(csv_bytes))

        from hiblooms_calibration import (   # type: ignore
            prepare_insitu,
            compute_satellite_features,
            match_insitu_to_overpass,
            fit_calibration_model,
            build_diagnostics_figure,
            _pack_download_bytes,
        )

        insitu_clean, priority_dates = prepare_insitu(
            df_raw, target_variable, start_hour, end_hour
        )

        # ── Paso 2: Geometría ────────────────────────────────────────────
        update(job_id, "Cargando geometría del embalse…", 15)
        gdf, aoi = _aoi_from_geojson(aoi_geojson)

        min_date   = pd.to_datetime(insitu_clean["date"]).min().strftime("%Y-%m-%d")
        max_date   = pd.to_datetime(insitu_clean["date"]).max().strftime("%Y-%m-%d")

        # ── Paso 3: Extraer features de satélite ─────────────────────────
        update(job_id, "Extrayendo predictores Sentinel-2…", 25)
        sat_df = compute_satellite_features(
            aoi=aoi,
            start_date=min_date,
            end_date=max_date,
            max_cloud_percentage=max_cloud,
            min_coverage_percentage=min_coverage,
            scale_m=20,
            candidate_indices=predictor_set,
            priority_dates=priority_dates,
        )

        # ── Paso 4: Match in-situ con sobrepaso ──────────────────────────
        update(job_id, "Emparejando datos in-situ con sobrepaso satelital…", 60)
        insitu_daily = match_insitu_to_overpass(
            insitu_clean, sat_df, overpass_window_hours=overpass_window
        )

        # ── Paso 5: Entrenar modelos ─────────────────────────────────────
        update(job_id, "Entrenando modelos de calibración…", 75)
        result = fit_calibration_model(
            insitu_daily_df=insitu_daily,
            sat_df=sat_df,
            target_variable=target_variable,
            predictor_set=predictor_set,
            candidate_models=selected_models,
            outlier_method=outlier_method,
            cv_scheme=cv_scheme,
            cv_folds=5,
            test_size=0.25,
            min_samples_required=min_samples,
        )

        # ── Paso 6: Generar figura de diagnóstico ────────────────────────
        update(job_id, "Generando gráficos de diagnóstico…", 90)
        fig = build_diagnostics_figure(
            result["predictions_df"],
            f"Best model: {result['config']['best_model_name']}",
        )
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        diag_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        # Archivos de descarga en base64
        download_bytes = _pack_download_bytes(result)
        downloads_b64 = {
            name: base64.b64encode(data).decode("utf-8")
            for name, data in download_bytes.items()
        }

        # ── Paso 7: Devolver resultados ──────────────────────────────────
        update(job_id, "Finalizando…", 98)
        results = {
            "config":             result["config"],
            "metrics_df":         result["metrics_df"].to_dict(orient="records"),
            "predictions_df":     result["predictions_df"].to_dict(orient="records"),
            "removed_outliers_df":result["removed_outliers_df"].to_dict(orient="records"),
            "diagnostics_png_b64":diag_b64,
            "download_files":     downloads_b64,
        }
        complete(job_id, results)
        log.info(f"[{job_id}] Calibration job completed")

    except Exception as e:
        log.error(f"[{job_id}] Calibration job failed: {e}", exc_info=True)
        fail(job_id, str(e))
