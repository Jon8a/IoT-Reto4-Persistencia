"""
probe_api.py — Prueba qué endpoint acepta Metabase para añadir tarjetas al dashboard.
"""
import requests, os

URL   = os.getenv("METABASE_URL", "http://localhost:3000")
EMAIL = os.getenv("METABASE_EMAIL", "admin@windturbine.com")
PASS  = os.getenv("METABASE_PASSWORD", "WindTurbine2024!")

s = requests.Session()
r = s.post(f"{URL}/api/session", json={"username": EMAIL, "password": PASS})
s.headers.update({"X-Metabase-Session": r.json()["id"]})
print("Sesión iniciada")

# Ver la versión de Metabase
v = s.get(f"{URL}/api/health").json()
print(f"Health: {v}")

props = s.get(f"{URL}/api/session/properties").json()
print(f"Versión Metabase: {props.get('version', {})}")

# Buscar el primer dashboard existente
dashs = s.get(f"{URL}/api/dashboard").json()
print(f"Dashboards: {[(d['id'], d['name']) for d in dashs]}")

# Buscar una tarjeta (card) existente para probar
cards = s.get(f"{URL}/api/card").json()
if cards:
    test_card_id = cards[0]["id"]
    print(f"Usando card de prueba id={test_card_id}")

    # Probar el endpoint más genérico y ver el status
    for dash in dashs:
        did = dash["id"]
        # Obtener las dashcards actuales del dashboard
        d_detail = s.get(f"{URL}/api/dashboard/{did}").json()
        print(f"\nDashboard {did} dashcards actuales: {[dc.get('id') for dc in d_detail.get('dashcards', d_detail.get('ordered_cards', []))]}")
        
        # Intentar con /dashcard
        r1 = s.post(f"{URL}/api/dashboard/{did}/dashcard", json={
            "card_id": test_card_id, "row": 5, "col": 0, "size_x": 4, "size_y": 3
        })
        print(f"POST /dashboard/{did}/dashcard → {r1.status_code}: {r1.text[:150]}")
        
        # Si falla, intentar con /cards
        r2 = s.post(f"{URL}/api/dashboard/{did}/cards", json={
            "cardId": test_card_id, "row": 5, "col": 0, "size_x": 4, "size_y": 3
        })
        print(f"POST /dashboard/{did}/cards → {r2.status_code}: {r2.text[:150]}")
        break
