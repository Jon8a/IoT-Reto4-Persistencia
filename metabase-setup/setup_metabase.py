"""
setup_metabase.py — Configura Metabase automáticamente via API REST:
  1. Espera a que Metabase arranque
  2. Completa el setup inicial (crea usuario admin + organización)
  3. Conecta QuestDB como fuente de datos (via protocolo PostgreSQL)
  4. Crea preguntas (queries/gráficos) y un dashboard con todos los gráficos

Uso:
  python metabase-setup/setup_metabase.py

Requisitos:
  pip install requests
"""

import requests
import time
import sys
import os

# ── Configuración de Metabase ──────────────────────────────────────────────────
# URL y credenciales del administrador de Metabase.
# Se pueden sobreescribir con variables de entorno para uso en Docker.
URL_METABASE      = os.getenv("METABASE_URL", "http://localhost:3000")
EMAIL_ADMIN       = os.getenv("METABASE_EMAIL", "admin@windturbine.com")
CONTRASENA_ADMIN  = os.getenv("METABASE_PASSWORD", "WindTurbine2024!")
NOMBRE_ORG        = "Wind Turbine IoT"

# ── Configuración de QuestDB (fuente de datos) ─────────────────────────────────
# En Docker usamos el nombre del servicio ("questdb") como host.
# En local se usa "localhost". Se sobreescribe con QUESTDB_MB_HOST si es necesario.
HOST_QDB    = os.getenv("QUESTDB_MB_HOST", "questdb")   # nombre del servicio en Docker
PUERTO_QDB  = int(os.getenv("QUESTDB_PG_PORT", 8812))
USUARIO_QDB = os.getenv("QUESTDB_USER", "admin")
PASS_QDB    = os.getenv("QUESTDB_PASS", "quest")
BD_QDB      = "qdb"
# ──────────────────────────────────────────────────────────────────────────────

# Sesión HTTP persistente: mantiene cabeceras (token de autenticación) entre llamadas
sesion = requests.Session()
TOKEN  = None


def log(mensaje: str):
    """Imprime un mensaje con sangría, con flush inmediato para ver el progreso en tiempo real."""
    print(f"  {mensaje}", flush=True)


# ── 1. Esperar a que Metabase esté disponible ──────────────────────────────────
def esperar_metabase(timeout=180):
    """
    Hace polling al endpoint /api/health de Metabase hasta que responda 'ok'
    o hasta agotar el timeout (por defecto 3 minutos).

    Metabase tarda en arrancar porque inicializa su base de datos interna (H2/Postgres).
    Si no responde en el tiempo indicado, el script termina con error.
    """
    print("⏳ Esperando a que Metabase arranque...")
    limite_tiempo = time.time() + timeout
    while time.time() < limite_tiempo:
        try:
            respuesta = requests.get(f"{URL_METABASE}/api/health", timeout=5)
            if respuesta.status_code == 200 and respuesta.json().get("status") == "ok":
                print("✓ Metabase listo\n")
                return True
        except Exception:
            pass  # Metabase aún no responde, seguir esperando
        time.sleep(3)
        print("  ...", end="", flush=True)
    print("\n✗ Timeout esperando Metabase")
    sys.exit(1)


# ── 2. Setup inicial de Metabase ───────────────────────────────────────────────
def configuracion_inicial():
    """
    Completa el asistente de configuración inicial de Metabase via API.

    Flujo:
      1. Solicita el token de setup único que Metabase genera solo la primera vez.
      2. Si ya no existe (setup completado), inicia sesión directamente.
      3. Si existe, envía el payload con los datos del admin y las preferencias.
      4. Guarda el token de sesión en la cabecera de la sesión HTTP para las siguientes llamadas.
    """
    print("🔧 Configurando Metabase...")

    # El token de setup solo está disponible antes del primer arranque
    respuesta = sesion.get(f"{URL_METABASE}/api/session/properties")
    token_setup = respuesta.json().get("setup-token")

    if not token_setup:
        # Metabase ya fue configurado en un arranque anterior
        log("Setup ya completado, iniciando sesión...")
        return iniciar_sesion()

    # Payload con los datos del administrador y las preferencias de la organización
    datos_setup = {
        "token": token_setup,
        "user": {
            "email":      EMAIL_ADMIN,
            "password":   CONTRASENA_ADMIN,
            "first_name": "Admin",
            "last_name":  "IoT",
            "site_name":  NOMBRE_ORG,
        },
        "prefs": {
            "site_name":      NOMBRE_ORG,
            "allow_tracking": False,   # desactivar telemetría
        },
        "database": None,  # la BD se conecta en un paso posterior
    }

    respuesta = sesion.post(f"{URL_METABASE}/api/setup", json=datos_setup)
    if respuesta.status_code != 200:
        # Puede ocurrir si Metabase ya fue configurado en un arranque previo
        # pero el token de setup aún aparece como válido. En ese caso, hacemos login.
        log(f"Setup rechazado ({respuesta.status_code}), intentando login directo...")
        return iniciar_sesion()

    # Guardar el token de sesión en la cabecera para autenticar las siguientes llamadas
    token_sesion = respuesta.json().get("id")
    sesion.headers.update({"X-Metabase-Session": token_sesion})
    log(f"Admin creado: {EMAIL_ADMIN} / {CONTRASENA_ADMIN}")
    return token_sesion


def iniciar_sesion():
    """
    Inicia sesión en Metabase con las credenciales del admin y guarda el token
    en la cabecera de la sesión HTTP compartida.
    """
    global TOKEN
    respuesta = sesion.post(f"{URL_METABASE}/api/session",
                            json={"username": EMAIL_ADMIN, "password": CONTRASENA_ADMIN})
    token_sesion = respuesta.json().get("id")
    sesion.headers.update({"X-Metabase-Session": token_sesion})
    TOKEN = token_sesion
    log("Sesión iniciada")
    return token_sesion


# ── 3. Conectar QuestDB como fuente de datos ───────────────────────────────────
def conectar_questdb():
    """
    Registra QuestDB en Metabase como una base de datos tipo 'postgres'.

    QuestDB tiene compatibilidad con el wire protocol de PostgreSQL en el puerto 8812,
    por lo que Metabase puede conectarse a él como si fuera una BD PostgreSQL estándar.

    Si la conexión ya existe (mismo nombre), devuelve su ID sin crear una duplicada.
    Tras crear la conexión, lanza un sync de esquema para que Metabase descubra la tabla.
    """
    print("🗄️  Conectando QuestDB...")

    # Verificar si ya existe una conexión con ese nombre para evitar duplicados
    bases_datos = sesion.get(f"{URL_METABASE}/api/database").json()
    existente = [bd for bd in bases_datos.get("data", []) if bd.get("name") == "QuestDB - Wind Turbine"]
    if existente:
        id_bd = existente[0]["id"]
        log(f"QuestDB ya conectado (id={id_bd})")
        return id_bd

    # Payload de configuración de la nueva fuente de datos
    configuracion_bd = {
        "engine": "postgres",          # QuestDB usa el driver de PostgreSQL
        "name":   "QuestDB - Wind Turbine",
        "details": {
            "host":     HOST_QDB,
            "port":     PUERTO_QDB,
            "dbname":   BD_QDB,
            "user":     USUARIO_QDB,
            "password": PASS_QDB,
            "ssl":      False,
        },
        "auto_run_queries": True,      # ejecutar queries automáticamente al abrir una pregunta
        "is_full_sync":     True,      # sincronizar todos los metadatos de la tabla
        "is_on_demand":     False,
    }

    respuesta = sesion.post(f"{URL_METABASE}/api/database", json=configuracion_bd)
    if respuesta.status_code not in (200, 202):
        log(f"Error conectando BD: {respuesta.text}")
        sys.exit(1)

    id_bd = respuesta.json()["id"]
    log(f"QuestDB conectado (id={id_bd})")

    # Lanzar sincronización de esquema para que Metabase detecte la tabla wind_turbine
    log("Sincronizando esquema...")
    sesion.post(f"{URL_METABASE}/api/database/{id_bd}/sync_schema")
    time.sleep(8)   # esperar a que el sync termine antes de buscar la tabla
    return id_bd


# ── 4. Obtener el ID de la tabla en Metabase ──────────────────────────────────
def obtener_id_tabla(id_bd: int, nombre_tabla="wind_turbine") -> int:
    """
    Busca la tabla 'wind_turbine' en los metadatos de la BD y devuelve su ID interno de Metabase.

    Reintenta hasta 10 veces con 6 segundos de espera entre intentos,
    ya que el sync puede tardar dependiendo del volumen de datos.
    Si no la encuentra, termina el script con un mensaje de ayuda.
    """
    print("🔍 Buscando tabla wind_turbine...")
    for intento in range(10):
        metadatos = sesion.get(f"{URL_METABASE}/api/database/{id_bd}/metadata").json()
        for tabla in metadatos.get("tables", []):
            if tabla["name"].lower() == nombre_tabla.lower():
                log(f"Tabla encontrada (id={tabla['id']})")
                return tabla["id"]
        log(f"Intento {intento+1}/10 — esperando sync...")
        # Volver a lanzar sync por si el anterior no terminó
        sesion.post(f"{URL_METABASE}/api/database/{id_bd}/sync_schema")
        time.sleep(6)
    log("✗ No se encontró la tabla. Asegúrate de haber insertado datos primero.")
    sys.exit(1)


def obtener_ids_campos(id_tabla: int) -> dict:
    """
    Consulta los metadatos de la tabla y devuelve un diccionario {nombre_campo: id_campo}.
    Útil para construir queries estructuradas (MBQL) en lugar de SQL nativo.
    """
    metadatos = sesion.get(f"{URL_METABASE}/api/table/{id_tabla}/query_metadata").json()
    return {campo["name"]: campo["id"] for campo in metadatos.get("fields", [])}


# ── 5. Crear colección (carpeta) en Metabase ──────────────────────────────────
def crear_coleccion() -> int:
    """
    Crea una colección (equivalente a una carpeta) en Metabase para agrupar
    todas las preguntas y el dashboard del proyecto.

    Si ya existe una colección con ese nombre, devuelve su ID directamente.
    """
    print("📁 Creando colección...")
    colecciones = sesion.get(f"{URL_METABASE}/api/collection").json()
    existente = [c for c in colecciones if c.get("name") == "Wind Turbine IoT"]
    if existente:
        id_coleccion = existente[0]["id"]
        log(f"Colección ya existe (id={id_coleccion})")
        return id_coleccion

    # Crear la colección con un color azul corporativo
    respuesta = sesion.post(f"{URL_METABASE}/api/collection",
                            json={"name": "Wind Turbine IoT", "color": "#509EE3"})
    id_coleccion = respuesta.json()["id"]
    log(f"Colección creada (id={id_coleccion})")
    return id_coleccion


# ── 6. Crear preguntas individuales (gráficos) ────────────────────────────────
def crear_pregunta(nombre: str, id_bd: int, sql_nativo: str, tipo_grafico: str,
                   configuracion_viz: dict, id_coleccion: int) -> int:
    """
    Crea una 'pregunta' (card) en Metabase usando SQL nativo contra QuestDB.

    Si ya existe una pregunta con el mismo nombre en la colección, devuelve su ID.
    Las preguntas son los bloques de visualización individuales que se añaden al dashboard.

    Parámetros:
      nombre          → nombre visible de la pregunta en Metabase.
      id_bd           → ID de la fuente de datos en Metabase.
      sql_nativo      → consulta SQL a ejecutar.
      tipo_grafico    → tipo de visualización: 'line', 'bar', 'scalar', etc.
      configuracion_viz → opciones de ejes, colores, métricas, etc.
      id_coleccion    → carpeta donde se guardará la pregunta.
    """
    # Verificar si la pregunta ya existe para evitar duplicados
    tarjetas = sesion.get(f"{URL_METABASE}/api/card",
                          params={"collection_id": id_coleccion}).json()
    existente = [t for t in tarjetas if t.get("name") == nombre]
    if existente:
        log(f"  Ya existe: {nombre}")
        return existente[0]["id"]

    datos_pregunta = {
        "name":          nombre,
        "display":       tipo_grafico,
        "collection_id": id_coleccion,
        "dataset_query": {
            "type":     "native",    # usamos SQL nativo (no el constructor visual MBQL)
            "database": id_bd,
            "native":   {"query": sql_nativo},
        },
        "visualization_settings": configuracion_viz,
    }
    respuesta = sesion.post(f"{URL_METABASE}/api/card", json=datos_pregunta)
    if respuesta.status_code not in (200, 202):
        log(f"  Error creando '{nombre}': {respuesta.text[:200]}")
        return None
    id_tarjeta = respuesta.json()["id"]
    log(f"  Creada: {nombre} (id={id_tarjeta})")
    return id_tarjeta


def crear_todas_las_preguntas(id_bd: int, id_coleccion: int) -> list:
    """
    Crea las 9 preguntas (gráficos) del proyecto y devuelve la lista de sus IDs.

    Contenido:
      [0] Serie temporal de potencia activa diaria (línea)
      [1] Serie temporal de velocidad de viento diaria (línea)
      [2] Producción media por hora del día (barras)
      [3] Factor de capacidad mensual (barras)
      [4] Curva de potencia real vs teórica (línea)
      [5] Eficiencia mensual real vs teórica (línea)
      [6] KPI: total de registros (escalar)
      [7] KPI: potencia media global (escalar)
      [8] KPI: viento medio global (escalar)
    """
    print("📊 Creando preguntas (gráficos)...")
    lista_tarjetas = []

    # 1. Potencia activa a lo largo del tiempo (gráfico de línea diario)
    tarjeta = crear_pregunta(
        nombre="Potencia activa — serie temporal diaria",
        id_bd=id_bd,
        sql_nativo="""
            SELECT timestamp, round(avg(potencia_activa_kw), 2) AS potencia_media_kw
            FROM wind_turbine
            SAMPLE BY 1d
        """,
        tipo_grafico="line",
        configuracion_viz={
            "graph.x_axis.title_text": "Fecha",
            "graph.y_axis.title_text": "Potencia (kW)",
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["potencia_media_kw"],
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 2. Velocidad de viento a lo largo del tiempo (gráfico de línea diario)
    tarjeta = crear_pregunta(
        nombre="Velocidad del viento — serie temporal diaria",
        id_bd=id_bd,
        sql_nativo="""
            SELECT timestamp, round(avg(velocidad_viento_ms), 2) AS viento_medio_ms
            FROM wind_turbine
            SAMPLE BY 1d
        """,
        tipo_grafico="line",
        configuracion_viz={
            "graph.x_axis.title_text": "Fecha",
            "graph.y_axis.title_text": "Viento (m/s)",
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["viento_medio_ms"],
            "graph.colors":     ["#F9CF48"],   # amarillo para distinguirlo del azul de potencia
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 3. Producción media por hora del día (barras agrupadas por hora 0–23)
    tarjeta = crear_pregunta(
        nombre="Producción media por hora del día",
        id_bd=id_bd,
        sql_nativo="""
            SELECT hour(timestamp) AS hora,
                   round(avg(potencia_activa_kw), 2) AS potencia_media_kw
            FROM wind_turbine
            GROUP BY hora
            ORDER BY hora
        """,
        tipo_grafico="bar",
        configuracion_viz={
            "graph.dimensions": ["hora"],
            "graph.metrics":    ["potencia_media_kw"],
            "graph.x_axis.title_text": "Hora del día",
            "graph.y_axis.title_text": "Potencia media (kW)",
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 4. Factor de capacidad mensual (potencia real / nominal × 100)
    tarjeta = crear_pregunta(
        nombre="Factor de capacidad mensual (%)",
        id_bd=id_bd,
        sql_nativo="""
            SELECT timestamp,
                   round(avg(potencia_activa_kw) / 1500.0 * 100, 1) AS factor_capacidad_pct
            FROM wind_turbine
            SAMPLE BY 1M
        """,
        tipo_grafico="bar",
        configuracion_viz={
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["factor_capacidad_pct"],
            "graph.x_axis.title_text": "Mes",
            "graph.y_axis.title_text": "Factor de capacidad (%)",
            "graph.colors": ["#84BB4C"],   # verde para energía/eficiencia
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 5. Curva de potencia: real vs teórica (agrupada por velocidad de viento)
    # Este gráfico muestra si la turbina rinde según la curva del fabricante
    tarjeta = crear_pregunta(
        nombre="Curva de potencia: real vs teórica",
        id_bd=id_bd,
        sql_nativo="""
            SELECT round(velocidad_viento_ms, 1)              AS viento_ms,
                   round(avg(potencia_activa_kw), 1)       AS potencia_real_kw,
                   round(avg(potencia_teorica_kwh), 1) AS potencia_teorica_kwh
            FROM wind_turbine
            GROUP BY viento_ms
            ORDER BY viento_ms
        """,
        tipo_grafico="line",
        configuracion_viz={
            "graph.dimensions": ["viento_ms"],
            "graph.metrics":    ["potencia_real_kw", "potencia_teorica_kwh"],
            "graph.x_axis.title_text": "Velocidad del viento (m/s)",
            "graph.y_axis.title_text": "Potencia (kW / KWh)",
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 6. Eficiencia mensual (potencia real / teórica × 100)
    tarjeta = crear_pregunta(
        nombre="Eficiencia real vs teórica por mes (%)",
        id_bd=id_bd,
        sql_nativo="""
            SELECT timestamp,
                   round(avg(potencia_activa_kw) / avg(potencia_teorica_kwh) * 100, 1) AS eficiencia_pct
            FROM wind_turbine
            SAMPLE BY 1M
        """,
        tipo_grafico="line",
        configuracion_viz={
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["eficiencia_pct"],
            "graph.x_axis.title_text": "Mes",
            "graph.y_axis.title_text": "Eficiencia (%)",
            "graph.colors": ["#EF8C8C"],   # rojo suave para alertar si baja la eficiencia
        },
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 7. KPI escalar: número total de registros insertados en QuestDB
    tarjeta = crear_pregunta(
        nombre="Total de registros insertados",
        id_bd=id_bd,
        sql_nativo="SELECT count() AS total_registros FROM wind_turbine",
        tipo_grafico="scalar",   # muestra un único número grande
        configuracion_viz={},
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 8. KPI escalar: potencia media global de toda la serie histórica
    tarjeta = crear_pregunta(
        nombre="Potencia media global (kW)",
        id_bd=id_bd,
        sql_nativo="SELECT round(avg(potencia_activa_kw), 1) AS potencia_media_kw FROM wind_turbine",
        tipo_grafico="scalar",
        configuracion_viz={},
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # 9. KPI escalar: velocidad media del viento de toda la serie histórica
    tarjeta = crear_pregunta(
        nombre="Viento medio global (m/s)",
        id_bd=id_bd,
        sql_nativo="SELECT round(avg(velocidad_viento_ms), 2) AS viento_medio_ms FROM wind_turbine",
        tipo_grafico="scalar",
        configuracion_viz={},
        id_coleccion=id_coleccion,
    )
    lista_tarjetas.append(tarjeta)

    # Filtrar posibles None si alguna pregunta falló al crearse
    return [t for t in lista_tarjetas if t]


# ── 7. Crear dashboard y añadir las tarjetas ──────────────────────────────────
def crear_dashboard(id_coleccion: int, ids_tarjetas: list) -> int:
    """
    Crea el dashboard principal y coloca las tarjetas en un layout de cuadrícula.

    Metabase usa un sistema de grid de 18 columnas.
    Disposición del layout:
      Fila 0 (row=0):  3 KPIs escalares en columnas 0–5, 6–11, 12–17
      Fila 1 (row=3):  serie temporal de potencia (ancho completo)
      Fila 2 (row=9):  viento (izq.) + producción por hora (der.)
      Fila 3 (row=15): factor capacidad (izq.) + eficiencia (der.)
      Fila 4 (row=21): curva potencia real vs teórica (ancho completo)

    Si el dashboard ya existe, devuelve su ID sin crear uno nuevo.
    """
    print("🖥️  Creando dashboard...")

    # Comprobar si el dashboard ya fue creado en una ejecución anterior
    dashboards = sesion.get(f"{URL_METABASE}/api/dashboard",
                            params={"collection_id": id_coleccion}).json()
    existente = [d for d in dashboards if d.get("name") == "Wind Turbine — Dashboard Principal"]
    if existente:
        id_dashboard = existente[0]["id"]
        log(f"Dashboard ya existe (id={id_dashboard})")
        return id_dashboard

    # Crear el dashboard vacío
    respuesta = sesion.post(f"{URL_METABASE}/api/dashboard", json={
        "name":          "Wind Turbine — Dashboard Principal",
        "description":   "IoT Industrial — Facultad de Ingeniería Deusto",
        "collection_id": id_coleccion,
    })
    id_dashboard = respuesta.json()["id"]
    log(f"Dashboard creado (id={id_dashboard})")

    # Definir el layout: cada elemento indica qué tarjeta va en qué posición del grid
    # row/col son la posición, size_x/size_y son el tamaño en unidades del grid
    disposicion = [
        # Fila 0: KPIs (total registros, potencia media, viento medio) — 3 columnas iguales
        {"id_tarjeta": ids_tarjetas[6], "fila": 0,  "col": 0,  "ancho": 6,  "alto": 3},
        {"id_tarjeta": ids_tarjetas[7], "fila": 0,  "col": 6,  "ancho": 6,  "alto": 3},
        {"id_tarjeta": ids_tarjetas[8], "fila": 0,  "col": 12, "ancho": 6,  "alto": 3},
        # Fila 1: serie temporal potencia activa (ocupa todo el ancho)
        {"id_tarjeta": ids_tarjetas[0], "fila": 3,  "col": 0,  "ancho": 18, "alto": 6},
        # Fila 2: viento (mitad izq.) + producción por hora (mitad der.)
        {"id_tarjeta": ids_tarjetas[1], "fila": 9,  "col": 0,  "ancho": 9,  "alto": 6},
        {"id_tarjeta": ids_tarjetas[2], "fila": 9,  "col": 9,  "ancho": 9,  "alto": 6},
        # Fila 3: factor de capacidad (mitad izq.) + eficiencia (mitad der.)
        {"id_tarjeta": ids_tarjetas[3], "fila": 15, "col": 0,  "ancho": 9,  "alto": 6},
        {"id_tarjeta": ids_tarjetas[5], "fila": 15, "col": 9,  "ancho": 9,  "alto": 6},
        # Fila 4: curva de potencia real vs teórica (todo el ancho)
        {"id_tarjeta": ids_tarjetas[4], "fila": 21, "col": 0,  "ancho": 18, "alto": 6},
    ]

    # En Metabase ≥ v0.47 ya no existe POST individual por tarjeta.
    # Se usa PUT /api/dashboard/{id}/cards con TODAS las tarjetas de golpe.
    # Las tarjetas nuevas llevan "id": -1 (identificador temporal).
    tarjetas_payload = []
    for i, elemento in enumerate(disposicion):
        tarjetas_payload.append({
            "id":      -(i + 1),             # negativo = tarjeta nueva
            "card_id": elemento["id_tarjeta"],
            "row":     elemento["fila"],
            "col":     elemento["col"],
            "size_x":  elemento["ancho"],
            "size_y":  elemento["alto"],
            "parameter_mappings":     [],
            "visualization_settings": {},
        })

    respuesta = sesion.put(
        f"{URL_METABASE}/api/dashboard/{id_dashboard}/cards",
        json={"cards": tarjetas_payload},
    )
    if respuesta.status_code not in (200, 202):
        log(f"  Error añadiendo tarjetas al dashboard: {respuesta.text[:300]}")
    else:
        log(f"  {len(tarjetas_payload)} tarjetas añadidas correctamente")

    log("Todas las tarjetas añadidas al dashboard")
    return id_dashboard


# ── Punto de entrada principal ─────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n╔══════════════════════════════════════════╗")
    print("║   Metabase Auto-Setup — Wind Turbine IoT ║")
    print("╚══════════════════════════════════════════╝\n")

    # Paso 1: esperar a que Metabase esté listo
    esperar_metabase()

    # Paso 2: crear el usuario admin y completar el wizard inicial
    configuracion_inicial()

    # Paso 3: registrar QuestDB como fuente de datos
    id_bd = conectar_questdb()

    # Paso 4: crear la colección (carpeta) que agrupa todo el proyecto
    id_coleccion = crear_coleccion()

    # Paso 5: crear las 9 preguntas/gráficos
    ids_tarjetas = crear_todas_las_preguntas(id_bd, id_coleccion)

    # Paso 6: crear el dashboard y colocar los gráficos en el grid
    id_dashboard = crear_dashboard(id_coleccion, ids_tarjetas)

    # Resumen final con los enlaces de acceso
    print(f"\n✅ Setup completado.")
    print(f"   Dashboard → {URL_METABASE}/dashboard/{id_dashboard}")
    print(f"   Usuario   → {EMAIL_ADMIN}")
    print(f"   Password  → {CONTRASENA_ADMIN}")
    print()
    print("   💡 Activa el refresco automático en Metabase (botón reloj)")
    print("      y lanza  python src/ingest.py  para ver los datos en vivo\n")
