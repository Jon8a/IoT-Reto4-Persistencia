"""
queries.py — Agregaciones sobre los datos de la turbina en QuestDB.
Requiere: pip install psycopg2-binary pandas tabulate
"""

import psycopg2
import pandas as pd
import os

# ── Conexión via PostgreSQL wire protocol ──────────────────
CONN = dict(
    host=os.getenv("QUESTDB_HOST", "localhost"),
    port=int(os.getenv("QUESTDB_PG_PORT", 8812)),
    user=os.getenv("QUESTDB_USER", "admin"),
    password=os.getenv("QUESTDB_PASS", "quest"),
    database="qdb",
)
# ───────────────────────────────────────────────────────────


def get_conn():
    return psycopg2.connect(**CONN)


def run(query: str) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(query, conn)


def fmt(df: pd.DataFrame, title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)
    print(df.to_string(index=False))


# ── 1. Resumen general ──────────────────────────────────────
def resumen_general():
    q = """
    SELECT
        count()                             AS total_registros,
        round(avg(active_power_kw), 2)      AS potencia_media_kw,
        round(max(active_power_kw), 2)      AS potencia_max_kw,
        round(min(wind_speed_ms), 2)        AS viento_min_ms,
        round(max(wind_speed_ms), 2)        AS viento_max_ms,
        round(avg(wind_speed_ms), 2)        AS viento_medio_ms
    FROM wind_turbine
    """
    fmt(run(q), "Resumen general")


# ── 2. Producción media por hora del día ───────────────────
def produccion_por_hora():
    q = """
    SELECT
        hour(timestamp)                     AS hora,
        round(avg(active_power_kw), 2)      AS potencia_media_kw,
        round(avg(wind_speed_ms), 2)        AS viento_medio_ms
    FROM wind_turbine
    GROUP BY hora
    ORDER BY hora
    """
    fmt(run(q), "Producción media por hora del día")


# ── 3. Producción diaria (SAMPLE BY — sintaxis QuestDB) ────
def produccion_diaria():
    q = """
    SELECT
        timestamp,
        round(sum(active_power_kw) / 6, 2)  AS energia_kwh,
        round(avg(wind_speed_ms), 2)         AS viento_medio_ms,
        round(max(active_power_kw), 2)       AS pico_kw
    FROM wind_turbine
    SAMPLE BY 1d
    LIMIT 30
    """
    fmt(run(q), "Producción diaria (últimos 30 días en dataset)")


# ── 4. Factor de capacidad mensual ─────────────────────────
def factor_capacidad_mensual():
    """
    Factor de capacidad = energía real / energía teórica máxima
    Potencia nominal de la turbina: ~1500 kW (estimado del dataset)
    """
    q = """
    SELECT
        timestamp,
        round(avg(active_power_kw), 2)                          AS potencia_real_kw,
        round(avg(theoretical_power_kwh), 2)                    AS potencia_teorica_kwh,
        round(avg(active_power_kw) / 1500.0 * 100, 1)          AS factor_capacidad_pct
    FROM wind_turbine
    SAMPLE BY 1M
    """
    fmt(run(q), "Factor de capacidad mensual (potencia nominal ~1500 kW)")


# ── 5. Eficiencia: real vs teórica ─────────────────────────
def eficiencia_real_vs_teorica():
    q = """
    SELECT
        timestamp,
        round(avg(active_power_kw), 2)                                          AS real_kw,
        round(avg(theoretical_power_kwh), 2)                                    AS teorica_kwh,
        round(avg(active_power_kw) / avg(theoretical_power_kwh) * 100, 1)      AS eficiencia_pct
    FROM wind_turbine
    SAMPLE BY 1M
    """
    fmt(run(q), "Eficiencia real vs teórica por mes")


# ── 6. Distribución por rangos de viento ───────────────────
def distribucion_por_viento():
    q = """
    SELECT
        CASE
            WHEN wind_speed_ms < 3  THEN '0–3 m/s  (parada)'
            WHEN wind_speed_ms < 7  THEN '3–7 m/s  (baja)'
            WHEN wind_speed_ms < 11 THEN '7–11 m/s (media)'
            WHEN wind_speed_ms < 15 THEN '11–15 m/s (alta)'
            ELSE                         '>15 m/s  (muy alta)'
        END                                         AS rango_viento,
        count()                                     AS registros,
        round(avg(active_power_kw), 2)              AS potencia_media_kw
    FROM wind_turbine
    GROUP BY rango_viento
    ORDER BY rango_viento
    """
    fmt(run(q), "Distribución por rangos de velocidad de viento")


if __name__ == "__main__":
    print("Ejecutando agregaciones sobre QuestDB...\n")
    try:
        resumen_general()
        produccion_por_hora()
        produccion_diaria()
        factor_capacidad_mensual()
        eficiencia_real_vs_teorica()
        distribucion_por_viento()
        print("\n✓ Todas las agregaciones completadas.")
    except Exception as e:
        print(f"\nError conectando a QuestDB: {e}")
        print("Asegúrate de que QuestDB está corriendo: docker compose up -d")
