"""
reset_dashboard.py — Borra el dashboard y la colección de Metabase para forzar su recreación.
Ejecutar dentro de la red Docker o con METABASE_URL apuntando a localhost:3000.
"""
import requests, os, sys

URL   = os.getenv("METABASE_URL", "http://localhost:3000")
EMAIL = os.getenv("METABASE_EMAIL", "admin@windturbine.com")
PASS  = os.getenv("METABASE_PASSWORD", "WindTurbine2024!")

s = requests.Session()
r = s.post(f"{URL}/api/session", json={"username": EMAIL, "password": PASS})
if r.status_code != 200:
    print(f"Login fallido: {r.text}"); sys.exit(1)
s.headers.update({"X-Metabase-Session": r.json()["id"]})
print("Sesión iniciada")

# 1. Borrar todos los dashboards llamados "Wind Turbine — Dashboard Principal"
dashs = s.get(f"{URL}/api/dashboard").json()
for d in dashs:
    if "Wind Turbine" in d.get("name", ""):
        resp = s.delete(f"{URL}/api/dashboard/{d['id']}")
        print(f"  Dashboard {d['id']} '{d['name']}' borrado → {resp.status_code}")

# 2. Archivar la colección para forzar recreación de preguntas
cols = s.get(f"{URL}/api/collection").json()
for c in cols:
    if c.get("name") == "Wind Turbine IoT":
        resp = s.put(f"{URL}/api/collection/{c['id']}", json={"archived": True})
        print(f"  Colección {c['id']} archivada → {resp.status_code}")

print("\n✓ Listo. Vuelve a lanzar: docker compose rm -sf metabase-setup && docker compose up metabase-setup")
