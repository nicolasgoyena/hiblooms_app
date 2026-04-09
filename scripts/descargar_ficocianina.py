import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
import unicodedata
from sqlalchemy import create_engine
import time
import sys

def quitar_tildes(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')

def mostrar_cargando(texto, duracion=2):
    print(f"\n🔄 {texto}", end="")
    for _ in range(duracion):
        time.sleep(0.5)
        print(".", end="")
        sys.stdout.flush()
    print()

def descargar_y_subir_tramo(desde, hasta, engine):
    mostrar_cargando(f"Procesando tramo {desde} a {hasta}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    url = f"https://saica.chebro.es/fichaDataTabla.php?estacion=945&fini={desde}&ffin={hasta}"
    r = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")

    for t in soup.find_all("table"):
        try:
            df = pd.read_html(StringIO(str(t)))[0]
            df.columns = [quitar_tildes(c.strip()) for c in df.columns.astype(str)]

            if any("Ficocianina" in c for c in df.columns):
                # === PROFUNDIDAD desde HTML con clase f1/f2 ===
                cabecera_html = t.find("thead").find("tr")
                th_textos = [quitar_tildes(th.get_text(strip=True)) for th in cabecera_html.find_all("th")]
                try:
                    idx_profundidad = th_textos.index("Profundidad maxima embalse (m)")
                except ValueError:
                    print("⚠️ No se encontró la columna 'Profundidad maxima embalse (m)'")
                    idx_profundidad = None

                profundidad_col = []
                if idx_profundidad is not None:
                    filas = t.find("tbody").find_all("tr")
                    for row in filas:
                        celdas = row.find_all("td")
                        if len(celdas) <= idx_profundidad:
                            profundidad_col.append(None)
                            continue
                        celda = celdas[idx_profundidad]
                        clase = celda.get("class", [None])[0]
                        texto = celda.get_text(strip=True).replace(",", ".")
                        if clase != "f1" or not texto:
                            profundidad_col.append(None)
                        else:
                            try:
                                profundidad_col.append(float(texto))
                            except:
                                profundidad_col.append(None)
                    df["max_reservoir_depth"] = profundidad_col

                # Mapear columnas
                col_map = {}
                for col in df.columns:
                    if "Fecha" in col:
                        col_map[col] = "date_time"
                    elif "Ficocianina" in col:
                        col_map[col] = "phycocyanin"
                    elif "Temperatura" in col:
                        col_map[col] = "water_temp"
                    elif "Turbidez" in col:
                        col_map[col] = "turbidity"
                    elif col == "max_reservoir_depth":
                        col_map[col] = "max_reservoir_depth"

                df_filtrado = df[list(col_map.keys())].rename(columns=col_map)
                df_filtrado["date_time"] = pd.to_datetime(df_filtrado["date_time"], format="%d-%m-%Y %H:%M:%S", errors='coerce')
                df_filtrado.dropna(subset=["date_time", "phycocyanin"], inplace=True)
                df_filtrado.sort_values(by="date_time", inplace=True)

                # Añadir columna de geometría (PostGIS: WKT con SRID)
                wkt_geom = 'SRID=4326;POINT(-1.7883 41.8761)'
                df_sensor = pd.DataFrame({
                    "date_time": df_filtrado["date_time"],
                    "reservoir_code": "2351_VAL",
                    "geometry": wkt_geom,
                    "chlorophyll": None,
                    "phycocyanin": df_filtrado.get("phycocyanin"),
                    "water_temp": df_filtrado.get("water_temp"),
                    "ph": None,
                    "turbidity": df_filtrado.get("turbidity"),
                    "depth": df_filtrado.get("max_reservoir_depth")
                })

                df_sensor.to_sql('sensor_data', engine, if_exists='append', index=False, method='multi')
                print(f"✅ Datos de {desde} a {hasta} (insertados en sensor_data)")
                return True

        except Exception as e:
            print(f"❌ Error leyendo tabla de {desde} a {hasta}: {e}")
            continue

    print(f"❌ No se encontró la tabla deseada entre {desde} y {hasta}")
    return False

def iterar_periodos_y_guardar():
    # === Configuración de la base de datos Neon ===
    DB_USER = "neondb_owner"
    DB_PASS = "npg_ER4ilp6oScZP"
    DB_HOST = "ep-green-mud-abr4cbl7-pooler.eu-west-2.aws.neon.tech"
    DB_NAME = "neondb"

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?sslmode=require"
    )

    # === Resto del código igual ===
    fecha_inicio = datetime(2024, 3, 13)
    fecha_hoy = datetime.now()

    excl_inicio = datetime(2024, 11, 13)
    excl_fin = datetime(2024, 12, 11)

    while fecha_inicio < fecha_hoy:
        fecha_fin = fecha_inicio + timedelta(days=89)
        if fecha_fin > fecha_hoy:
            fecha_fin = fecha_hoy

        print(f"\n⏳ Evaluando tramo: {fecha_inicio.date()} a {fecha_fin.date()}")

        if fecha_fin < excl_inicio or fecha_inicio > excl_fin:
            fini = fecha_inicio.strftime('%d-%m-%Y')
            ffin = fecha_fin.strftime('%d-%m-%Y')
            print(f"📡 Descargando tramo válido: {fini} → {ffin}")
            descargar_y_subir_tramo(fini, ffin, engine)

        elif fecha_inicio < excl_inicio < fecha_fin:
            f1_ini = fecha_inicio
            f1_fin = excl_inicio - timedelta(days=1)
            f2_ini = excl_fin + timedelta(days=1)
            f2_fin = fecha_fin

            fini1 = f1_ini.strftime('%d-%m-%Y')
            ffin1 = f1_fin.strftime('%d-%m-%Y')
            print(f"📡 Descargando subtramo antes de exclusión: {fini1} → {ffin1}")
            descargar_y_subir_tramo(fini1, ffin1, engine)

            if f2_ini <= f2_fin:
                fini2 = f2_ini.strftime('%d-%m-%Y')
                ffin2 = f2_fin.strftime('%d-%m-%Y')
                print(f"📡 Descargando subtramo después de exclusión: {fini2} → {ffin2}")
                descargar_y_subir_tramo(fini2, ffin2, engine)

        elif excl_inicio <= fecha_inicio <= excl_fin < fecha_fin:
            f2_ini = excl_fin + timedelta(days=1)
            f2_fin = fecha_fin
            fini2 = f2_ini.strftime('%d-%m-%Y')
            ffin2 = f2_fin.strftime('%d-%m-%Y')
            print(f"📡 Descargando subtramo después de exclusión: {fini2} → {ffin2}")
            descargar_y_subir_tramo(fini2, ffin2, engine)

        else:
            print(f"⏭️  Tramo {fecha_inicio.date()} a {fecha_fin.date()} excluido completamente")

        fecha_inicio = fecha_fin + timedelta(days=1)

    print("\n✅ Todos los tramos procesados correctamente.")

# Ejecutar
iterar_periodos_y_guardar()

