import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
import unicodedata
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from geoalchemy2 import Geometry, WKTElement
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

def descargar_ultimo_dia_y_insertar(engine):
    fecha = (datetime.utcnow() - timedelta(days=1)).strftime('%d-%m-%Y')
    print(f"\n🔄 Descargando datos del {fecha}..")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    url = f"https://saica.chebro.es/fichaDataTabla.php?estacion=945&fini={fecha}&ffin={fecha}"
    r = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")

    for t in soup.find_all("table"):
        try:
            df = pd.read_html(StringIO(str(t)))[0]
            df.columns = [quitar_tildes(c.strip()) for c in df.columns.astype(str)]

            if any("Ficocianina" in c for c in df.columns):
                # === PROCESAR PROFUNDIDAD DESDE HTML CON CLASE f1/f2 ===
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
                df_filtrado["date_time"] = pd.to_datetime(df_filtrado["date_time"], format="%d-%m-%Y %H:%M:%S", utc=True, errors='coerce')
                df_filtrado.dropna(subset=["date_time", "phycocyanin"], inplace=True)
                df_filtrado.sort_values(by="date_time", inplace=True)

                geom = WKTElement('POINT(-1.7883 41.8761)', srid=4326)
                df_sensor = pd.DataFrame({
                    "date_time": df_filtrado["date_time"],
                    "chlorophyll": None,
                    "phycocyanin": df_filtrado.get("phycocyanin"),
                    "water_temp": df_filtrado.get("water_temp"),
                    "depth": df_filtrado.get("max_reservoir_depth"),
                    "ph": None,
                    "turbidity": df_filtrado.get("turbidity"),
                    "geometry": [geom] * len(df_filtrado),
                    "reservoir_code": "2351_VAL"
                })

                df_sensor.to_sql(
                    'sensor_data',
                    engine,
                    if_exists='append',
                    index=False,
                    dtype={
                        'geometry': Geometry('POINT', srid=4326),
                        'date_time': TIMESTAMP(timezone=True)
                    }
                )
                print(f"✅ Datos insertados correctamente ({len(df_sensor)} filas)")

                with engine.begin() as conn:
                    conn.execute(text("""
                        DELETE FROM sensor_data a
                        USING sensor_data b
                        WHERE a.ctid > b.ctid
                        AND a.date_time = b.date_time
                        AND a.reservoir_code = b.reservoir_code
                        AND a.phycocyanin IS NOT DISTINCT FROM b.phycocyanin
                        AND a.water_temp IS NOT DISTINCT FROM b.water_temp
                        AND a.depth IS NOT DISTINCT FROM b.depth
                        AND a.ph IS NOT DISTINCT FROM b.ph
                        AND a.turbidity IS NOT DISTINCT FROM b.turbidity
                        AND ST_Equals(a.geometry, b.geometry);
                    """))
                    print("🧹 Duplicados eliminados correctamente.")

                    conn.execute(text("""
                        CREATE TEMP TABLE temp_sensor_data AS
                        SELECT * FROM sensor_data ORDER BY date_time;

                        DELETE FROM sensor_data;
                        INSERT INTO sensor_data
                        SELECT * FROM temp_sensor_data;

                        DROP TABLE temp_sensor_data;
                    """))
                    print("📅 Tabla reordenada cronológicamente.")

                return

        except Exception as e:
            print(f"❌ Error procesando datos del {fecha}: {e}")
            return

    print(f"❌ No se encontró la tabla deseada para {fecha}")

# Conexión a la base de datos NEON
DB_USER = "neondb_owner"
DB_PASS = "npg_ER4ilp6oScZP"
DB_HOST = "ep-green-mud-abr4cbl7-pooler.eu-west-2.aws.neon.tech"
DB_NAME = "neondb"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?sslmode=require"
)

# Ejecutar función
descargar_ultimo_dia_y_insertar(engine)

