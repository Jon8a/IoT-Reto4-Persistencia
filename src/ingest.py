"""
ingest.py — Lee T1.csv e inserta los datos en QuestDB en tiempo real.

Modos de inserción:
  python src/ingest.py             → modo demo (lento, visible en Metabase)
  python src/ingest.py --fast      → modo rápido (carga completa en segundos)
  python src/ingest.py --delay 0.5 → delay personalizado en segundos por fila

En modo demo, cada fila se inserta individualmente con un pequeño delay,
de forma que Metabase puede ir refrescando y se ve la inserción en vivo.
"""

import pandas as pd
import socket
from datetime import datetime
import time
import os
import argparse

# ── Configuración de conexión a QuestDB ────────────────────────────────────────
# Se leen de variables de entorno para mayor flexibilidad (ej. en Docker).
# Si no están definidas, se usan valores por defecto para ejecución local.
HOST_QUESTDB  = os.getenv("QUESTDB_HOST", "localhost")
PUERTO_QUESTDB = int(os.getenv("QUESTDB_PORT", 9009))

# Ruta al CSV con los datos de la turbina eólica
RUTA_CSV      = os.path.join(os.path.dirname(__file__), "../data/T1.csv")

# Nombre de la tabla en QuestDB donde se almacenarán los datos
NOMBRE_TABLA  = "wind_turbine"

# ── Parámetros del modo demo (inserción lenta, visible en Metabase) ────────────
DELAY_DEMO    = 0.1   # segundos de espera entre cada fila insertada
BATCH_DEMO    = 1     # número de filas enviadas por cada transmisión en modo demo

# ── Parámetros del modo rápido (inserción masiva sin esperas) ──────────────────
BATCH_RAPIDO  = 1000  # número de filas enviadas por cada transmisión en modo rápido
# ──────────────────────────────────────────────────────────────────────────────


def parsear_argumentos():
    """
    Parsea los argumentos de línea de comandos.

    Permite al usuario elegir entre:
      --fast          → modo rápido (batches grandes, sin delay)
      --delay <seg>   → delay personalizado entre filas (modo demo ajustable)
    Si no se indica nada, se usa el modo demo con DELAY_DEMO por defecto.
    """
    parser = argparse.ArgumentParser(description="Ingestión de datos de turbina en QuestDB")
    # El grupo mutually_exclusive garantiza que no se puedan pasar --fast y --delay simultáneamente
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--fast",  action="store_true", help="Inserción rápida en batches")
    grupo.add_argument("--delay", type=float, default=None, help="Delay en segundos por fila (default demo: 0.1)")
    return parser.parse_args()


def cargar_y_preparar(ruta_csv: str) -> pd.DataFrame:
    """
    Carga el CSV y lo prepara para ser insertado en QuestDB.

    Pasos:
      1. Lee el CSV con pandas.
      2. Convierte la columna 'Date/Time' a un objeto datetime.
      3. Desplaza todos los timestamps al momento actual (para que los datos
         aparezcan 'en vivo' en Metabase, aunque sean históricos).
      4. Renombra las columnas al formato snake_case usado en QuestDB.
      5. Devuelve solo las columnas necesarias.
    """
    print("Cargando dataset...")
    datos = pd.read_csv(ruta_csv)

    # Convertir la columna de fecha al tipo datetime (formato: "01 01 2018 00:00")
    datos["timestamp"] = pd.to_datetime(datos["Date/Time"], format="%d %m %Y %H:%M")

    # Calcular el desfase entre el último registro del CSV y el momento actual (UTC)
    # y aplicarlo a todos los timestamps para simular datos en tiempo real
    desfase = datetime.utcnow() - datos["timestamp"].iloc[-1]
    datos["timestamp"] = datos["timestamp"] + desfase

    # Renombrar columnas del CSV a nombres más compactos para QuestDB
    datos = datos.rename(columns={
        "LV ActivePower (kW)":           "potencia_activa_kw",
        "Wind Speed (m/s)":              "velocidad_viento_ms",
        "Theoretical_Power_Curve (KWh)": "potencia_teorica_kwh",
        "Wind Direction (°)":            "direccion_viento_deg",
    })

    # Retornar solo las columnas relevantes para la tabla de QuestDB
    return datos[["timestamp", "potencia_activa_kw", "velocidad_viento_ms",
                  "potencia_teorica_kwh", "direccion_viento_deg"]]


def fila_a_ilp(fila) -> str:
    """
    Convierte una fila del DataFrame al formato ILP (InfluxDB Line Protocol).

    QuestDB acepta datos en este formato a través del puerto TCP 9009.
    Estructura: <tabla> <campo1>=<valor1>,<campo2>=<valor2> <timestamp_ns>
    El timestamp debe estar en nanosegundos desde Unix epoch.
    """
    # Convertir el timestamp de la fila a nanosegundos (requerido por ILP)
    ts_ns = int(fila["timestamp"].timestamp() * 1_000_000_000)
    return (
        f"{NOMBRE_TABLA} "
        f"potencia_activa_kw={fila['potencia_activa_kw']},"
        f"velocidad_viento_ms={fila['velocidad_viento_ms']},"
        f"potencia_teorica_kwh={fila['potencia_teorica_kwh']},"
        f"direccion_viento_deg={fila['direccion_viento_deg']} "
        f"{ts_ns}\n"
    )


def mostrar_fila_en_vivo(fila, insertadas, total):
    """
    Muestra en consola una barra de progreso y los valores de la última fila insertada.

    La barra tiene 50 caracteres de ancho y se actualiza en la misma línea (\r).
    Se muestran: porcentaje, timestamp, potencia activa, velocidad del viento
    y dirección del viento.
    """
    porcentaje = insertadas / total * 100
    # Construir la barra de progreso con bloques llenos y vacíos
    barra = "█" * int(porcentaje / 2) + "░" * (50 - int(porcentaje / 2))
    ts    = fila["timestamp"].strftime("%Y-%m-%d %H:%M")
    print(
        f"\r[{barra}] {porcentaje:5.1f}% | "
        f"{ts} | "
        f"kW: {fila['potencia_activa_kw']:7.1f} | "
        f"m/s: {fila['velocidad_viento_ms']:5.2f} | "
        f"dir: {fila['direccion_viento_deg']:6.1f}deg",
        end="", flush=True
    )


def ingestar(datos: pd.DataFrame, tamanio_batch: int, delay: float):
    """
    Conecta a QuestDB por TCP y envía los datos en batches usando el protocolo ILP.

    Parámetros:
      datos         → DataFrame con las filas a insertar.
      tamanio_batch → cuántas filas se agrupan por envío.
      delay         → segundos de espera tras cada batch (0 = modo rápido).

    Flujo:
      1. Abre un socket TCP hacia QuestDB.
      2. Itera sobre los datos en bloques de 'tamanio_batch' filas.
      3. Convierte cada bloque a ILP y lo envía por el socket.
      4. Muestra el progreso en consola tras cada envío.
      5. Si hay delay, espera proporcionalmente al número de filas del batch.
    """
    total     = len(datos)
    insertadas = 0
    inicio    = time.time()

    modo = "DEMO (lento, visible en vivo)" if delay > 0 else "RÁPIDO"
    print(f"\nModo: {modo}")
    print(f"Filas a insertar: {total:,} | Batch: {tamanio_batch} | Delay: {delay}s/fila")
    print(f"Conectando a QuestDB en {HOST_QUESTDB}:{PUERTO_QUESTDB}...\n")

    # Abrir conexión TCP con QuestDB (protocolo ILP en puerto 9009)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST_QUESTDB, PUERTO_QUESTDB))
        print(f"OK Conectado. Comenzando insercion...\n")

        # Recorrer el DataFrame en bloques del tamaño indicado
        for inicio_batch in range(0, total, tamanio_batch):
            batch   = datos.iloc[inicio_batch:inicio_batch + tamanio_batch]
            # Convertir todas las filas del batch a texto ILP y concatenarlas
            payload = "".join(fila_a_ilp(f) for _, f in batch.iterrows())
            # Enviar el payload codificado en bytes por el socket
            sock.sendall(payload.encode())
            insertadas += len(batch)

            # Actualizar la barra de progreso con los datos de la última fila del batch
            mostrar_fila_en_vivo(batch.iloc[-1], insertadas, total)

            # En modo demo, esperar proporcionalmente al batch para no saturar
            if delay > 0:
                time.sleep(delay * len(batch))

    # Calcular tiempo total y mostrar resumen final
    tiempo_total = time.time() - inicio
    print(f"\n\nOK Ingestion completa.")
    print(f"  {insertadas:,} filas insertadas en {tiempo_total:.1f}s ({insertadas/tiempo_total:.0f} filas/s)")
    print(f"  Rango: {datos['timestamp'].iloc[0].strftime('%Y-%m-%d')} -> {datos['timestamp'].iloc[-1].strftime('%Y-%m-%d')}")
    print(f"\n  QuestDB Web UI  -> http://localhost:9000")
    print(f"  Metabase        -> http://localhost:3000")


if __name__ == "__main__":
    # Paso 1: leer argumentos de línea de comandos
    args = parsear_argumentos()

    # Paso 2: configurar batch y delay según el modo seleccionado
    if args.fast:
        # Modo rápido: batches grandes, sin espera entre envíos
        tamanio_batch, delay = BATCH_RAPIDO, 0.0
        print("[RAPIDO] Modo rapido activado")
    else:
        # Modo demo: una fila a la vez con delay visible en Metabase
        delay = args.delay if args.delay is not None else DELAY_DEMO
        tamanio_batch = BATCH_DEMO
        print(f"[DEMO] Modo demo activado (delay={delay}s por fila)")
        print("   Abre Metabase en http://localhost:3000 y activa el refresco automatico")

    # Paso 3: cargar y preparar el CSV
    datos = cargar_y_preparar(RUTA_CSV)
    print(f"   Dataset: {len(datos):,} filas | "
          f"{datos['timestamp'].iloc[0].strftime('%Y-%m-%d')} -> {datos['timestamp'].iloc[-1].strftime('%Y-%m-%d')}\n")

    # Paso 4: lanzar la ingestión con los parámetros configurados
    ingestar(datos, tamanio_batch=tamanio_batch, delay=delay)
