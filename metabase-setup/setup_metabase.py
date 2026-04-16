"""
setup_metabase.py — Configura Metabase automáticamente via API:
  1. Espera a que Metabase arranque
  2. Completa el setup inicial (admin + org)
  3. Conecta QuestDB como fuente de datos
  4. Crea preguntas (queries) y un dashboard con todos los gráficos

Uso:
  python metabase-setup/setup_metabase.py

Requisitos:
  pip install requests
"""

import requests
import time
import sys
import os

# ── Config ─────────────────────────────────────────────────
MB_URL      = os.getenv("METABASE_URL", "http://localhost:3000")
MB_EMAIL    = os.getenv("METABASE_EMAIL", "admin@windturbine.com")
MB_PASSWORD = os.getenv("METABASE_PASSWORD", "WindTurbine2024!")
MB_NAME     = "Wind Turbine IoT"

QDB_HOST    = os.getenv("QUESTDB_MB_HOST", "questdb")   # nombre del servicio Docker
QDB_PORT    = int(os.getenv("QUESTDB_PG_PORT", 8812))
QDB_USER    = os.getenv("QUESTDB_USER", "admin")
QDB_PASS    = os.getenv("QUESTDB_PASS", "quest")
QDB_DB      = "qdb"
# ───────────────────────────────────────────────────────────

session = requests.Session()
TOKEN   = None


def log(msg: str):
    print(f"  {msg}", flush=True)


# ── 1. Esperar a Metabase ───────────────────────────────────
def wait_for_metabase(timeout=180):
    print("⏳ Esperando a que Metabase arranque...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{MB_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("✓ Metabase listo\n")
                return True
        except Exception:
            pass
        time.sleep(3)
        print("  ...", end="", flush=True)
    print("\n✗ Timeout esperando Metabase")
    sys.exit(1)


# ── 2. Setup inicial ────────────────────────────────────────
def initial_setup():
    print("🔧 Configurando Metabase...")

    # Obtener token de setup
    r = session.get(f"{MB_URL}/api/session/properties")
    setup_token = r.json().get("setup-token")

    if not setup_token:
        log("Setup ya completado, iniciando sesión...")
        return login()

    payload = {
        "token": setup_token,
        "user": {
            "email":      MB_EMAIL,
            "password":   MB_PASSWORD,
            "first_name": "Admin",
            "last_name":  "IoT",
            "site_name":  MB_NAME,
        },
        "prefs": {
            "site_name":    MB_NAME,
            "allow_tracking": False,
        },
        "database": None,
    }

    r = session.post(f"{MB_URL}/api/setup", json=payload)
    if r.status_code != 200:
        log(f"Error en setup: {r.text}")
        sys.exit(1)

    token = r.json().get("id")
    session.headers.update({"X-Metabase-Session": token})
    log(f"Admin creado: {MB_EMAIL} / {MB_PASSWORD}")
    return token


def login():
    global TOKEN
    r = session.post(f"{MB_URL}/api/session",
                     json={"username": MB_EMAIL, "password": MB_PASSWORD})
    token = r.json().get("id")
    session.headers.update({"X-Metabase-Session": token})
    TOKEN = token
    log("Sesión iniciada")
    return token


# ── 3. Conectar QuestDB ─────────────────────────────────────
def connect_questdb():
    print("🗄️  Conectando QuestDB...")

    # Comprobar si ya existe
    dbs = session.get(f"{MB_URL}/api/database").json()
    existing = [d for d in dbs.get("data", []) if d.get("name") == "QuestDB - Wind Turbine"]
    if existing:
        db_id = existing[0]["id"]
        log(f"QuestDB ya conectado (id={db_id})")
        return db_id

    payload = {
        "engine": "postgres",
        "name":   "QuestDB - Wind Turbine",
        "details": {
            "host":     QDB_HOST,
            "port":     QDB_PORT,
            "dbname":   QDB_DB,
            "user":     QDB_USER,
            "password": QDB_PASS,
            "ssl":      False,
        },
        "auto_run_queries":     True,
        "is_full_sync":         True,
        "is_on_demand":         False,
    }

    r = session.post(f"{MB_URL}/api/database", json=payload)
    if r.status_code not in (200, 202):
        log(f"Error conectando BD: {r.text}")
        sys.exit(1)

    db_id = r.json()["id"]
    log(f"QuestDB conectado (id={db_id})")

    # Lanzar sync para que Metabase descubra la tabla
    log("Sincronizando esquema...")
    session.post(f"{MB_URL}/api/database/{db_id}/sync_schema")
    time.sleep(8)   # dar tiempo al sync
    return db_id


# ── 4. Obtener tabla ────────────────────────────────────────
def get_table_id(db_id: int, table_name="wind_turbine") -> int:
    print("🔍 Buscando tabla wind_turbine...")
    for attempt in range(10):
        tables = session.get(f"{MB_URL}/api/database/{db_id}/metadata").json()
        for t in tables.get("tables", []):
            if t["name"].lower() == table_name.lower():
                log(f"Tabla encontrada (id={t['id']})")
                return t["id"]
        log(f"Intento {attempt+1}/10 — esperando sync...")
        session.post(f"{MB_URL}/api/database/{db_id}/sync_schema")
        time.sleep(6)
    log("✗ No se encontró la tabla. Asegúrate de haber insertado datos primero.")
    sys.exit(1)


def get_field_ids(table_id: int) -> dict:
    """Devuelve un dict campo_nombre -> field_id"""
    meta = session.get(f"{MB_URL}/api/table/{table_id}/query_metadata").json()
    return {f["name"]: f["id"] for f in meta.get("fields", [])}


# ── 5. Crear colección ──────────────────────────────────────
def create_collection() -> int:
    print("📁 Creando colección...")
    cols = session.get(f"{MB_URL}/api/collection").json()
    existing = [c for c in cols if c.get("name") == "Wind Turbine IoT"]
    if existing:
        cid = existing[0]["id"]
        log(f"Colección ya existe (id={cid})")
        return cid
    r = session.post(f"{MB_URL}/api/collection",
                     json={"name": "Wind Turbine IoT", "color": "#509EE3"})
    cid = r.json()["id"]
    log(f"Colección creada (id={cid})")
    return cid


# ── 6. Crear preguntas ──────────────────────────────────────
def create_question(name: str, db_id: int, native_sql: str, display: str,
                    viz_settings: dict, collection_id: int) -> int:
    # Comprobar si ya existe
    cards = session.get(f"{MB_URL}/api/card",
                        params={"collection_id": collection_id}).json()
    existing = [c for c in cards if c.get("name") == name]
    if existing:
        log(f"  Ya existe: {name}")
        return existing[0]["id"]

    payload = {
        "name":            name,
        "display":         display,
        "collection_id":   collection_id,
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native":   {"query": native_sql},
        },
        "visualization_settings": viz_settings,
    }
    r = session.post(f"{MB_URL}/api/card", json=payload)
    if r.status_code not in (200, 202):
        log(f"  Error creando '{name}': {r.text[:200]}")
        return None
    card_id = r.json()["id"]
    log(f"  Creada: {name} (id={card_id})")
    return card_id


def create_all_questions(db_id: int, collection_id: int) -> list:
    print("📊 Creando preguntas (gráficos)...")
    cards = []

    # 1. Potencia activa a lo largo del tiempo (línea)
    c = create_question(
        name="Potencia activa — serie temporal diaria",
        db_id=db_id,
        native_sql="""
            SELECT timestamp, round(avg(active_power_kw), 2) AS potencia_media_kw
            FROM wind_turbine
            SAMPLE BY 1d
        """,
        display="line",
        viz_settings={
            "graph.x_axis.title_text": "Fecha",
            "graph.y_axis.title_text": "Potencia (kW)",
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["potencia_media_kw"],
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 2. Velocidad de viento — serie temporal
    c = create_question(
        name="Velocidad del viento — serie temporal diaria",
        db_id=db_id,
        native_sql="""
            SELECT timestamp, round(avg(wind_speed_ms), 2) AS viento_medio_ms
            FROM wind_turbine
            SAMPLE BY 1d
        """,
        display="line",
        viz_settings={
            "graph.x_axis.title_text": "Fecha",
            "graph.y_axis.title_text": "Viento (m/s)",
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["viento_medio_ms"],
            "graph.colors":     ["#F9CF48"],
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 3. Producción media por hora del día (barras)
    c = create_question(
        name="Producción media por hora del día",
        db_id=db_id,
        native_sql="""
            SELECT hour(timestamp) AS hora,
                   round(avg(active_power_kw), 2) AS potencia_media_kw
            FROM wind_turbine
            GROUP BY hora
            ORDER BY hora
        """,
        display="bar",
        viz_settings={
            "graph.dimensions": ["hora"],
            "graph.metrics":    ["potencia_media_kw"],
            "graph.x_axis.title_text": "Hora del día",
            "graph.y_axis.title_text": "Potencia media (kW)",
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 4. Factor de capacidad mensual (barras)
    c = create_question(
        name="Factor de capacidad mensual (%)",
        db_id=db_id,
        native_sql="""
            SELECT timestamp,
                   round(avg(active_power_kw) / 1500.0 * 100, 1) AS factor_capacidad_pct
            FROM wind_turbine
            SAMPLE BY 1M
        """,
        display="bar",
        viz_settings={
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["factor_capacidad_pct"],
            "graph.x_axis.title_text": "Mes",
            "graph.y_axis.title_text": "Factor de capacidad (%)",
            "graph.colors": ["#84BB4C"],
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 5. Curva de potencia real vs teórica (scatter/línea)
    c = create_question(
        name="Curva de potencia: real vs teórica",
        db_id=db_id,
        native_sql="""
            SELECT round(wind_speed_ms, 1)              AS viento_ms,
                   round(avg(active_power_kw), 1)       AS potencia_real_kw,
                   round(avg(theoretical_power_kwh), 1) AS potencia_teorica_kwh
            FROM wind_turbine
            GROUP BY viento_ms
            ORDER BY viento_ms
        """,
        display="line",
        viz_settings={
            "graph.dimensions": ["viento_ms"],
            "graph.metrics":    ["potencia_real_kw", "potencia_teorica_kwh"],
            "graph.x_axis.title_text": "Velocidad del viento (m/s)",
            "graph.y_axis.title_text": "Potencia (kW / KWh)",
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 6. Eficiencia mensual (línea)
    c = create_question(
        name="Eficiencia real vs teórica por mes (%)",
        db_id=db_id,
        native_sql="""
            SELECT timestamp,
                   round(avg(active_power_kw) / avg(theoretical_power_kwh) * 100, 1) AS eficiencia_pct
            FROM wind_turbine
            SAMPLE BY 1M
        """,
        display="line",
        viz_settings={
            "graph.dimensions": ["timestamp"],
            "graph.metrics":    ["eficiencia_pct"],
            "graph.x_axis.title_text": "Mes",
            "graph.y_axis.title_text": "Eficiencia (%)",
            "graph.colors": ["#EF8C8C"],
        },
        collection_id=collection_id,
    )
    cards.append(c)

    # 7. KPI: total de registros (número)
    c = create_question(
        name="Total de registros insertados",
        db_id=db_id,
        native_sql="SELECT count() AS total_registros FROM wind_turbine",
        display="scalar",
        viz_settings={},
        collection_id=collection_id,
    )
    cards.append(c)

    # 8. KPI: potencia media actual
    c = create_question(
        name="Potencia media global (kW)",
        db_id=db_id,
        native_sql="SELECT round(avg(active_power_kw), 1) AS potencia_media_kw FROM wind_turbine",
        display="scalar",
        viz_settings={},
        collection_id=collection_id,
    )
    cards.append(c)

    # 9. KPI: viento medio
    c = create_question(
        name="Viento medio global (m/s)",
        db_id=db_id,
        native_sql="SELECT round(avg(wind_speed_ms), 2) AS viento_medio_ms FROM wind_turbine",
        display="scalar",
        viz_settings={},
        collection_id=collection_id,
    )
    cards.append(c)

    return [c for c in cards if c]


# ── 7. Crear dashboard ──────────────────────────────────────
def create_dashboard(collection_id: int, card_ids: list) -> int:
    print("🖥️  Creando dashboard...")

    # Comprobar si ya existe
    dashs = session.get(f"{MB_URL}/api/dashboard",
                        params={"collection_id": collection_id}).json()
    existing = [d for d in dashs if d.get("name") == "Wind Turbine — Dashboard Principal"]
    if existing:
        did = existing[0]["id"]
        log(f"Dashboard ya existe (id={did})")
        return did

    r = session.post(f"{MB_URL}/api/dashboard", json={
        "name":          "Wind Turbine — Dashboard Principal",
        "description":   "IoT Industrial — Facultad de Ingeniería Deusto",
        "collection_id": collection_id,
    })
    did = r.json()["id"]
    log(f"Dashboard creado (id={did})")

    # Layout: KPIs arriba (cols 0-5, 6-11, 12-17), gráficos debajo
    layout = [
        # KPIs fila 0 — card_ids[6,7,8] = total, potencia_media, viento
        {"card_id": card_ids[6], "row": 0,  "col": 0,  "size_x": 6,  "size_y": 3},
        {"card_id": card_ids[7], "row": 0,  "col": 6,  "size_x": 6,  "size_y": 3},
        {"card_id": card_ids[8], "row": 0,  "col": 12, "size_x": 6,  "size_y": 3},
        # Fila 1 — serie temporal potencia (ancha)
        {"card_id": card_ids[0], "row": 3,  "col": 0,  "size_x": 18, "size_y": 6},
        # Fila 2 — viento + horas
        {"card_id": card_ids[1], "row": 9,  "col": 0,  "size_x": 9,  "size_y": 6},
        {"card_id": card_ids[2], "row": 9,  "col": 9,  "size_x": 9,  "size_y": 6},
        # Fila 3 — factor capacidad + eficiencia
        {"card_id": card_ids[3], "row": 15, "col": 0,  "size_x": 9,  "size_y": 6},
        {"card_id": card_ids[5], "row": 15, "col": 9,  "size_x": 9,  "size_y": 6},
        # Fila 4 — curva de potencia (ancha)
        {"card_id": card_ids[4], "row": 21, "col": 0,  "size_x": 18, "size_y": 6},
    ]

    for item in layout:
        r = session.post(f"{MB_URL}/api/dashboard/{did}/cards", json={
            "cardId":  item["card_id"],
            "row":     item["row"],
            "col":     item["col"],
            "size_x":  item["size_x"],
            "size_y":  item["size_y"],
            "parameter_mappings":    [],
            "visualization_settings": {},
        })
        if r.status_code not in (200, 202):
            log(f"  Error añadiendo card {item['card_id']}: {r.text[:100]}")

    log("Todas las cards añadidas al dashboard")
    return did


# ── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n╔══════════════════════════════════════════╗")
    print("║   Metabase Auto-Setup — Wind Turbine IoT ║")
    print("╚══════════════════════════════════════════╝\n")

    wait_for_metabase()
    initial_setup()
    db_id         = connect_questdb()
    collection_id = create_collection()
    card_ids      = create_all_questions(db_id, collection_id)
    dash_id       = create_dashboard(collection_id, card_ids)

    print(f"\n✅ Setup completado.")
    print(f"   Dashboard → {MB_URL}/dashboard/{dash_id}")
    print(f"   Usuario   → {MB_EMAIL}")
    print(f"   Password  → {MB_PASSWORD}")
    print()
    print("   💡 Activa el refresco automático en Metabase (botón reloj)")
    print("      y lanza  python src/ingest.py  para ver los datos en vivo\n")
