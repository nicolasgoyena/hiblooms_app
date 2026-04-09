import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO
import time

def descargar_clorofila_val_completa(salida_csv):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    resultados = []

    for año in range(2018, 2025):  # 2018 a 2024 inclusive
        for mes in range(1, 13):
            if año == 2024 and mes > 6:
                break  # Limitar hasta junio 2024

            url = f"https://saica.chebro.es/ElValExport.php?anyo={año}&mes={mes:02d}"
            print(f"🔄 Procesando: {url}")

            try:
                r = requests.get(url, headers=headers, timeout=30)
                soup = BeautifulSoup(r.text, "html.parser")

                for t in soup.find_all("table"):
                    try:
                        df = pd.read_html(StringIO(str(t)))[0]
                        df.columns = [c.strip() for c in df.columns.astype(str)]

                        if "Clorofila" in " ".join(df.columns):
                            # Crear datetime
                            df['Datetime'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'], errors='coerce')

                            # Filtrar entre 08:00 y 15:00
                            df_filtrado = df[df['Datetime'].dt.hour.between(0, 24)]

                            resultados.append(df_filtrado)

                    except Exception as e:
                        print(f"❌ Error leyendo tabla en {url}: {e}")
                        continue

                time.sleep(1)  # Evitar sobrecarga de peticiones

            except Exception as e:
                print(f"❌ Error al descargar {url}: {e}")
                continue

    if resultados:
        df_final = pd.concat(resultados, ignore_index=True)
        df_final.to_csv(salida_csv, index=False)
        print(f"✅ Datos guardados en {salida_csv}")
    else:
        print("⚠️ No se encontraron datos válidos")

# Ejecutar
salida_csv = "Clorofila_ElVal_2018_2024_total.csv"
descargar_clorofila_val_completa(salida_csv)
