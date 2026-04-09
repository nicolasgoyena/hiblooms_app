# -*- coding: utf-8 -*-
"""
Versión optimizada que devuelve fecha y nubosidad (%), con mensajes de progreso
"""

import ee
import geopandas as gpd
import pandas as pd
from datetime import datetime

# Inicializar Earth Engine
try:
    ee.Initialize('ee-nicolasgoyenaserveto')
except Exception:
    ee.Authenticate()
    ee.Initialize()

def get_valid_dates_fast(
    embalse,
    start_date,
    end_date,
    shape_path=None,
    cloud_thr=30,
    coverage_thr=0.5
):
    print(f"\n🔎 Iniciando análisis para el embalse '{embalse}' entre {start_date} y {end_date}...")
    
    if shape_path is None:
        shape_path = f"data/shapefiles/{embalse.lower()}.shp"

    gdf = gpd.read_file(shape_path).to_crs("EPSG:4326")
    geom = ee.Geometry.Polygon(gdf.geometry.iloc[0].__geo_interface__["coordinates"])

    def process_image(img):
        scl = img.select('SCL')
        cld_scl = scl.eq(7).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
        non_valid = scl.eq(4).Or(scl.eq(5))
        valid_mask = scl.mask().And(non_valid.Not())

        cloud_scl = cld_scl.updateMask(valid_mask).reduceRegion(
            reducer=ee.Reducer.mean(), geometry=geom, scale=20, maxPixels=1e13
        ).get('SCL')

        cloud_prob = img.select('MSK_CLDPRB').gte(10).updateMask(valid_mask).reduceRegion(
            reducer=ee.Reducer.mean(), geometry=geom, scale=20, maxPixels=1e13
        ).get('MSK_CLDPRB')

        total = ee.Image(1).clip(geom).reduceRegion(
            reducer=ee.Reducer.count(), geometry=geom, scale=20, maxPixels=1e13
        ).get("constant")

        valid = ee.Image(1).updateMask(img.select("B4").mask()).clip(geom).reduceRegion(
            reducer=ee.Reducer.count(), geometry=geom, scale=20, maxPixels=1e13
        ).get("constant")

        coverage_is_valid = ee.Algorithms.If(
            ee.Algorithms.IsEqual(total, None),
            False,
            ee.Algorithms.If(
                ee.Algorithms.IsEqual(valid, None),
                False,
                True
            )
        )

        coverage = ee.Algorithms.If(
            coverage_is_valid,
            ee.Number(valid).divide(total).multiply(100),
            0
        )

        cloud_ok = ee.Algorithms.If(
            ee.Algorithms.IsEqual(cloud_scl, None),
            ee.Algorithms.If(
                ee.Algorithms.IsEqual(cloud_prob, None),
                None,
                ee.Number(cloud_prob).multiply(100)
            ),
            ee.Algorithms.If(
                ee.Algorithms.IsEqual(cloud_prob, None),
                ee.Number(cloud_scl).multiply(100),
                ee.Number(cloud_scl).multiply(0.95).add(ee.Number(cloud_prob).multiply(0.05)).multiply(100)
            )
        )

        return img.set({
            'cloud': cloud_ok,
            'coverage': coverage,
            'date': img.date().format('YYYY-MM-dd')
        })

    coleccion = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(geom)
        .sort("system:time_start")
        .map(process_image)
    )

    coleccion_filtrada = coleccion.filter(ee.Filter.And(
        ee.Filter.lte('cloud', cloud_thr),
        ee.Filter.gte('coverage', coverage_thr * 100)
    ))

    print("📥 Consultando fechas válidas y nubosidad desde Earth Engine...")
    fechas = coleccion_filtrada.aggregate_array('date').getInfo()
    nubes = coleccion_filtrada.aggregate_array('cloud').getInfo()

    print(f"✅ Se han encontrado {len(fechas)} imágenes válidas. Generando lista final...")
    resultados = []
    for i, (f, n) in enumerate(zip(fechas, nubes), 1):
        print(f"  [{i}/{len(fechas)}] {f} → Nubosidad: {round(n, 2)}%")
        resultados.append({'fecha': f, 'nubosidad': round(n, 2)})

    return sorted(resultados, key=lambda x: x['fecha'])

# MAIN
import time

if __name__ == "__main__":
    inicio = time.time()

    fechas = get_valid_dates_fast(
        embalse="bellus",
        start_date="2017-07-01",
        end_date="2025-05-31",
        shape_path=r"C:\Users\ngoyenaserv\Desktop\PyCharmMiscProject\el_val_4326.shp",
        cloud_thr=60,
        coverage_thr=0.75
    )

    df = pd.DataFrame(fechas)
    df.to_csv("fechas_validas_el_val_historico.csv", index=False)
    print("\n💾 Resultados guardados en 'fechas_validas_el_val_rapido.csv'")

    fin = time.time()
    duracion = fin - inicio
    print(f"⏱️ Tiempo total de ejecución: {duracion:.2f} segundos")

