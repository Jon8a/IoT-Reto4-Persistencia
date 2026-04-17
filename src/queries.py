"""
queries.py — Agregaciones sobre los datos de la turbina en QuestDB.

Ejecuta varias consultas SQL analíticas sobre la tabla wind_turbine
y muestra los resultados formateados en la consola.

Requisitos: pip install psycopg2-binary pandas
"""

import psycopg2
import pandas as pd
import os

# ── Configuración de conexión a QuestDB via protocolo PostgreSQL ───────────────
# QuestDB expone el puerto 8812 compatible con el wire protocol de PostgreSQL,
# lo que permite usar psycopg2 directamente sin un driver especial.
# Los valores se leen de variables de entorno para facilitar el uso en Docker.
PARAMETROS_CONEXION = dict(
    host=os.getenv("QUESTDB_HOST", "localhost"),
    port=int(os.getenv("QUESTDB_PG_PORT", 8812)),
    user=os.getenv("QUESTDB_USER", "admin"),
    password=os.getenv("QUESTDB_PASS", "quest"),
    database="qdb",
)
# ──────────────────────────────────────────────────────────────────────────────


def obtener_conexion():
    """Abre y devuelve una conexión psycopg2 a QuestDB con los parámetros configurados."""
    return psycopg2.connect(**PARAMETROS_CONEXION)


def ejecutar_consulta(consulta: str) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL contra QuestDB y devuelve el resultado como DataFrame.

    Usa 'with' para garantizar que la conexión se cierra correctamente,
    incluso si ocurre un error durante la ejecución.
    """
    with obtener_conexion() as conexion:
        return pd.read_sql(consulta, conexion)


def mostrar_resultado(datos: pd.DataFrame, titulo: str):
    """
    Imprime un DataFrame en consola con un encabezado decorado.

    Parámetros:
      datos  → DataFrame con los resultados de la consulta.
      titulo → texto que aparece como título de la sección.
    """
    print(f"\n{'='*60}")
    print(f"  {titulo}")
    print('='*60)
    print(datos.to_string(index=False))


# ── 1. Resumen general ────────────────────────────────────────────────────────
def resumen_general():
    """
    Muestra estadísticas globales de toda la serie histórica:
    total de registros, potencia media/máxima y velocidad de viento mín/máx/media.
    """
    consulta = """
    SELECT
        count()                             AS total_registros,
        round(avg(potencia_activa_kw), 2)      AS potencia_media_kw,
        round(max(potencia_activa_kw), 2)      AS potencia_max_kw,
        round(min(velocidad_viento_ms), 2)        AS viento_min_ms,
        round(max(velocidad_viento_ms), 2)        AS viento_max_ms,
        round(avg(velocidad_viento_ms), 2)        AS viento_medio_ms
    FROM wind_turbine
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Resumen general")


# ── 2. Producción media por hora del día ──────────────────────────────────────
def produccion_por_hora():
    """
    Agrupa los registros por hora del día (0–23) y calcula la potencia media
    y el viento medio para cada franja horaria.
    Útil para identificar las horas pico de producción.
    """
    consulta = """
    SELECT
        hour(timestamp)                     AS hora,
        round(avg(potencia_activa_kw), 2)      AS potencia_media_kw,
        round(avg(velocidad_viento_ms), 2)        AS viento_medio_ms
    FROM wind_turbine
    GROUP BY hora
    ORDER BY hora
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Producción media por hora del día")


# ── 3. Producción diaria (SAMPLE BY — sintaxis nativa QuestDB) ────────────────
def produccion_diaria():
    """
    Usa la cláusula SAMPLE BY de QuestDB para agrupar por día automáticamente.

    La energía diaria en kWh se estima dividiendo la suma de potencia (kW) entre 6,
    ya que los datos tienen una granularidad de 10 minutos (6 muestras por hora).
    Se limita a los últimos 30 días del dataset.
    """
    consulta = """
    SELECT
        timestamp,
        round(sum(potencia_activa_kw) / 6, 2)  AS energia_kwh,
        round(avg(velocidad_viento_ms), 2)         AS viento_medio_ms,
        round(max(potencia_activa_kw), 2)       AS pico_kw
    FROM wind_turbine
    SAMPLE BY 1d
    LIMIT 30
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Producción diaria (últimos 30 días en dataset)")


# ── 4. Factor de capacidad mensual ────────────────────────────────────────────
def factor_capacidad_mensual():
    """
    Calcula el factor de capacidad mensual de la turbina.

    Factor de capacidad = potencia real media / potencia nominal × 100
    La potencia nominal estimada de esta turbina es ~1500 kW (extraída del dataset).
    Un factor del 30–40% es habitual en turbinas eólicas terrestres.
    """
    consulta = """
    SELECT
        timestamp,
        round(avg(potencia_activa_kw), 2)                          AS potencia_real_kw,
        round(avg(potencia_teorica_kwh), 2)                    AS potencia_teorica_kwh,
        round(avg(potencia_activa_kw) / 1500.0 * 100, 1)          AS factor_capacidad_pct
    FROM wind_turbine
    SAMPLE BY 1M
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Factor de capacidad mensual (potencia nominal ~1500 kW)")


# ── 5. Eficiencia: potencia real vs teórica ───────────────────────────────────
def eficiencia_real_vs_teorica():
    """
    Compara por mes la potencia media real generada con la curva teórica del fabricante.

    La eficiencia se expresa como porcentaje: (real / teórica) × 100.
    Valores < 80% pueden indicar pérdidas mecánicas, cortes o condiciones adversas.
    """
    consulta = """
    SELECT
        timestamp,
        round(avg(potencia_activa_kw), 2)                                          AS real_kw,
        round(avg(potencia_teorica_kwh), 2)                                    AS teorica_kwh,
        round(avg(potencia_activa_kw) / avg(potencia_teorica_kwh) * 100, 1)      AS eficiencia_pct
    FROM wind_turbine
    SAMPLE BY 1M
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Eficiencia real vs teórica por mes")


# ── 6. Distribución por rangos de velocidad de viento ────────────────────────
def distribucion_por_viento():
    """
    Clasifica los registros en 5 rangos de velocidad de viento y muestra
    cuántos registros hay en cada rango y la potencia media generada.

    Rangos:
      < 3 m/s  → parada (turbina sin producción)
      3–7 m/s  → baja producción
      7–11 m/s → producción media
      11–15 m/s → producción alta
      > 15 m/s → producción muy alta (o limitación por seguridad)
    """
    consulta = """
    SELECT
        CASE
            WHEN velocidad_viento_ms < 3  THEN '0-3 m/s  (parada)'
            WHEN velocidad_viento_ms < 7  THEN '3-7 m/s  (baja)'
            WHEN velocidad_viento_ms < 11 THEN '7-11 m/s (media)'
            WHEN velocidad_viento_ms < 15 THEN '11-15 m/s (alta)'
            ELSE                         '>15 m/s  (muy alta)'
        END                                         AS rango_viento,
        count()                                     AS registros,
        round(avg(potencia_activa_kw), 2)              AS potencia_media_kw
    FROM wind_turbine
    GROUP BY rango_viento
    ORDER BY rango_viento
    """
    mostrar_resultado(ejecutar_consulta(consulta), "Distribución por rangos de velocidad de viento")


if __name__ == "__main__":
    print("Ejecutando agregaciones sobre QuestDB...\n")
    try:
        # Ejecutar las seis consultas en secuencia
        resumen_general()
        produccion_por_hora()
        produccion_diaria()
        factor_capacidad_mensual()
        eficiencia_real_vs_teorica()
        distribucion_por_viento()
        print("\nOK Todas las agregaciones completadas.")
    except Exception as error:
        # Mostrar el error y una sugerencia de solución si QuestDB no está corriendo
        print(f"\nError conectando a QuestDB: {error}")
        print("Asegúrate de que QuestDB está corriendo: docker compose up -d")
