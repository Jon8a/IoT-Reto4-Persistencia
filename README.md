# 💨 Wind Turbine IoT — Persistencia de Datos

**Asignatura:** IoT Industrial — Universidad de Deusto  
---

## 👥 Miembros del equipo

- Jon Ochoa
- Oier Martinez


---

## 📋 Descripción

Proyecto de persistencia de datos para IoT Industrial. Se toma el dataset SCADA de una turbina de viento real (Kaggle) y se inserta en **QuestDB**, una base de datos de series temporales de alto rendimiento. Los dashboards se visualizan en **Metabase**, que se configura automáticamente al levantar el entorno.

---

## 🗂️ Estructura del proyecto

```
wind-turbine-iot/
├── data/
│   └── T1.csv                        # Dataset original (Kaggle)
├── src/
│   ├── ingest.py                     # Lee el CSV e inserta en QuestDB
│   └── queries.py                    # Agregaciones de demostración (consola)
├── metabase-setup/
│   └── setup_metabase.py             # Configura Metabase automáticamente via API
├── docker-compose.yml                # QuestDB + Metabase + auto-setup
├── requirements.txt
└── README.md
```

---

## 🚀 Instrucciones de uso

### Requisitos previos

- Docker y Docker Compose instalados
- Python 3.10+

### 1. Levantar todo el entorno

```bash
docker compose up
```

Esto arranca automáticamente:
1. **QuestDB** — base de datos de series temporales
2. **Metabase** — visualizador (tarda ~2 min en arrancar la primera vez)
3. **metabase-setup** — configura la conexión a QuestDB y crea todos los dashboards automáticamente

Los datos de Metabase se guardan en un volumen Docker (`metabase_data`), por lo que **la configuración persiste** entre reinicios. Solo hace el setup la primera vez.

### 2. Entorno virtual e instalación de dependencias

Es muy recomendable usar un entorno virtual para aislar las dependencias del proyecto.

```bash
# 1. Crear el entorno virtual (usar Python 3.10, 3.11 o 3.12)
python -m venv .venv

# 2. Activar el entorno virtual
# Si usas Windows (PowerShell):
.\.venv\Scripts\activate
# Si usas Linux/macOS:
source .venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt
```

### 3. Insertar los datos (tres modos)

```bash
# Modo demo: inserción lenta fila a fila, visible en vivo en Metabase
python src/ingest.py

# Delay personalizado (ej: 0.5 segundos por fila)
python src/ingest.py --delay 0.5

# Modo rápido: carga completa en segundos
python src/ingest.py --fast
```

### 4. Ver el dashboard

Abre **http://localhost:3000** en el navegador.

- Usuario: `admin@windturbine.com`
- Contraseña: `WindTurbine2024!`
- El dashboard "Wind Turbine — Dashboard Principal" ya estará creado

💡 Para ver la inserción **en vivo**: abre el dashboard → clic en el botón **reloj** (arriba a la derecha) → activa refresco automático cada 1 minuto. Luego lanza `python src/ingest.py`.

### 5. Ejecutar las agregaciones por consola (opcional)

```bash
python src/queries.py
```

---

## 📊 Dataset

- **Fuente:** [Kaggle — Wind Turbine SCADA Dataset](https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset)
- **Registros:** 50.530 (año 2018, medición cada 10 minutos)
- **Columnas:**

| Campo | Descripción |
|---|---|
| `Date/Time` | Timestamp (cada 10 min) |
| `LV ActivePower (kW)` | Potencia activa generada |
| `Wind Speed (m/s)` | Velocidad del viento |
| `Theoretical_Power_Curve (KWh)` | Potencia teórica esperada |
| `Wind Direction (°)` | Dirección del viento |

---

## 📈 Gráficos en el dashboard

| Gráfico | Tipo | Descripción |
|---|---|---|
| Total de registros | KPI | Contador en vivo durante la inserción |
| Potencia media global | KPI | Media de toda la serie |
| Viento medio global | KPI | Media de toda la serie |
| Potencia activa — serie temporal | Línea | Promedio diario a lo largo del año |
| Velocidad del viento — serie temporal | Línea | Promedio diario |
| Producción media por hora del día | Barras | Patrón intradiario |
| Factor de capacidad mensual | Barras | Potencia real / nominal (1500 kW) |
| Eficiencia real vs teórica | Línea | Comparativa mensual |
| Curva de potencia: real vs teórica | Línea | Relación viento-potencia |

---

## 🤔 ¿Por qué QuestDB?

| Criterio | QuestDB | PostgreSQL | MongoDB |
|---|---|---|---|
| Fit para series temporales | ✅ Nativo | ⚠️ Con extensión | ❌ No optimizado |
| Consultas temporales | `SAMPLE BY` nativo | Verbose | Aggregation pipeline |
| Rendimiento de inserción | ~1.6M filas/s | ~100k filas/s | ~200k filas/s |
| Compresión | Alta | Media | Media |
| Curva de aprendizaje | Baja (SQL) | Baja (SQL) | Media |
| Web UI incluida | ✅ Sí | ❌ No | ❌ No |

Los datos SCADA son inmutables, append-only y siempre se consultan por rango de tiempo, lo que hace a QuestDB la elección más adecuada.

---

## 🔒 Seguridad

Las credenciales se gestionan mediante variables de entorno en `docker-compose.yml`, nunca hardcodeadas en el código de aplicación. En producción se añadiría TLS/HTTPS y control de acceso por roles (RBAC).

---

## 🔧 Posibles vías de mejora

- Conectar una fuente de datos meteorológica en tiempo real (Open-Meteo API)
- Implementar alertas en Metabase cuando la potencia cae por debajo de un umbral
- Añadir modelos de predicción de producción (ML)
- Desplegar en la nube con autenticación real
- Añadir múltiples turbinas y comparativa entre ellas

---

## ⚠️ Problemas / Retos encontrados

- El formato de fecha del CSV (`01 01 2018 00:00`) requiere parsing manual con formato explícito
- QuestDB usa InfluxDB Line Protocol para ingestión masiva (puerto 9009), distinto al SQL (puerto 8812)
- Metabase necesita tiempo de arranque (~2 min) antes de poder configurarse via API
- El nombre de host de QuestDB dentro de Docker es `questdb` (nombre del servicio), no `localhost`

---

## 🔄 Alternativas posibles

- **Base de datos:** InfluxDB (más maduro), TimescaleDB (SQL puro), DuckDB (sin servidor)
- **Visualización:** Grafana (más potente y configurable), Apache Superset (BI completo), Streamlit (Python)
- **Ingestión:** Apache Kafka para datos en tiempo real real, en lugar de CSV estático
