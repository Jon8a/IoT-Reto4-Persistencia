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

# ── Configuración ──────────────────────────────────────────
QUESTDB_HOST  = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT  = int(os.getenv("QUESTDB_PORT", 9009))
CSV_PATH      = os.path.join(os.path.dirname(__file__), "../data/T1.csv")
TABLE_NAME    = "wind_turbine"

# Modo demo: inserción visible en vivo
DEMO_DELAY    = 0.1   # segundos entre cada fila en modo demo
DEMO_BATCH    = 1     # filas por envío en modo demo

# Modo rápido: inserción masiva
FAST_BATCH    = 1000  # filas por envío en modo rápido
# ───────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(description="Ingestión de datos de turbina en QuestDB")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--fast",  action="store_true", help="Inserción rápida en batches")
    group.add_argument("--delay", type=float, default=None, help="Delay en segundos por fila (default demo: 0.1)")
    return parser.parse_args()


def load_and_prepare(csv_path: str) -> pd.DataFrame:
    print("Cargando dataset...")
    df = pd.read_csv(csv_path)

    # Parsear timestamps (formato: "01 01 2018 00:00")
    df["timestamp"] = pd.to_datetime(df["Date/Time"], format="%d %m %Y %H:%M")

    # Desplazar timestamps al presente (los datos terminan ahora)
    delta = datetime.utcnow() - df["timestamp"].iloc[-1]
    df["timestamp"] = df["timestamp"] + delta

    df = df.rename(columns={
        "LV ActivePower (kW)":           "active_power_kw",
        "Wind Speed (m/s)":              "wind_speed_ms",
        "Theoretical_Power_Curve (KWh)": "theoretical_power_kwh",
        "Wind Direction (°)":            "wind_direction_deg",
    })

    return df[["timestamp", "active_power_kw", "wind_speed_ms",
               "theoretical_power_kwh", "wind_direction_deg"]]


def row_to_ilp(row) -> str:
    ts_ns = int(row["timestamp"].timestamp() * 1_000_000_000)
    return (
        f"{TABLE_NAME} "
        f"active_power_kw={row['active_power_kw']},"
        f"wind_speed_ms={row['wind_speed_ms']},"
        f"theoretical_power_kwh={row['theoretical_power_kwh']},"
        f"wind_direction_deg={row['wind_direction_deg']} "
        f"{ts_ns}\n"
    )


def print_live_row(row, inserted, total):
    """Muestra en consola los valores de la fila recién insertada."""
    pct = inserted / total * 100
    bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
    ts  = row["timestamp"].strftime("%Y-%m-%d %H:%M")
    print(
        f"\r[{bar}] {pct:5.1f}% | "
        f"{ts} | "
        f"⚡ {row['active_power_kw']:7.1f} kW | "
        f"💨 {row['wind_speed_ms']:5.2f} m/s | "
        f"🧭 {row['wind_direction_deg']:6.1f}°",
        end="", flush=True
    )


def ingest(df: pd.DataFrame, batch_size: int, delay: float):
    total    = len(df)
    inserted = 0
    start_t  = time.time()

    mode = "DEMO (lento, visible en vivo)" if delay > 0 else "RÁPIDO"
    print(f"\nModo: {mode}")
    print(f"Filas a insertar: {total:,} | Batch: {batch_size} | Delay: {delay}s/fila")
    print(f"Conectando a QuestDB en {QUESTDB_HOST}:{QUESTDB_PORT}...\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((QUESTDB_HOST, QUESTDB_PORT))
        print(f"✓ Conectado. Comenzando inserción...\n")

        for start in range(0, total, batch_size):
            batch   = df.iloc[start:start + batch_size]
            payload = "".join(row_to_ilp(r) for _, r in batch.iterrows())
            s.sendall(payload.encode())
            inserted += len(batch)

            # Mostrar última fila del batch en consola
            print_live_row(batch.iloc[-1], inserted, total)

            if delay > 0:
                time.sleep(delay * len(batch))

    elapsed = time.time() - start_t
    print(f"\n\n✓ Ingestión completa.")
    print(f"  {inserted:,} filas insertadas en {elapsed:.1f}s ({inserted/elapsed:.0f} filas/s)")
    print(f"  Rango: {df['timestamp'].iloc[0].strftime('%Y-%m-%d')} → {df['timestamp'].iloc[-1].strftime('%Y-%m-%d')}")
    print(f"\n  QuestDB Web UI  → http://localhost:9000")
    print(f"  Metabase        → http://localhost:3000")


if __name__ == "__main__":
    args  = parse_args()

    if args.fast:
        batch, delay = FAST_BATCH, 0.0
        print("⚡ Modo rápido activado")
    else:
        delay = args.delay if args.delay is not None else DEMO_DELAY
        batch = DEMO_BATCH
        print(f"🎬 Modo demo activado (delay={delay}s por fila)")
        print("   Abre Metabase en http://localhost:3000 y activa el refresco automático")

    df = load_and_prepare(CSV_PATH)
    print(f"   Dataset: {len(df):,} filas | "
          f"{df['timestamp'].iloc[0].strftime('%Y-%m-%d')} → {df['timestamp'].iloc[-1].strftime('%Y-%m-%d')}\n")
    ingest(df, batch_size=batch, delay=delay)
