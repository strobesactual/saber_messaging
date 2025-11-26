# app/config.py
from pathlib import Path
import os

# ---- Paths ----
# If this file sits in app/config.py, BASE_DIR is the project root.
BASE_DIR = Path(__file__).resolve().parent.parent
TRACKING_DIR = BASE_DIR / "tracking_data"
TRACKING_DIR.mkdir(parents=True, exist_ok=True)

# Device "latest" state is in SQLite (not CSV)
DB_PATH = TRACKING_DIR / "device_latest.db"

# Long-form flight log (append-only CSV) for customers
CSV_LOG_PATH = TRACKING_DIR / "kyberdyne_tracking.csv"

# Optional map artifacts
KML_PATH = TRACKING_DIR / "kyberdyne_tracking.kml"
GEOJSON_PATH = TRACKING_DIR / "kyberdyne_tracking.geojson"

# ---- Toggles (you can flip these later without code changes) ----
CSV_LOG_ENABLED = True          # keep customer flight log CSV
KML_EXPORT_ENABLED = True       # maintain KML
GEOJSON_EXPORT_ENABLED = True   # maintain GeoJSON

# ---- CoT / TAK Publisher (mostly via environment; these are fallbacks) ----
DEFAULT_COT_URL = "ssl://kyberdyne.ddns.net:8089"
# Set COT_URL to "" to disable the publisher entirely.
COT_URL = os.getenv("COT_URL", DEFAULT_COT_URL)
COT_DISABLED = os.getenv("COT_DISABLED", "")
# PKCS#12 material (client cert/key + truststore) to bootstrap TLS
COT_PKCS12_PATH = os.getenv("COT_PKCS12_PATH", str(BASE_DIR / "saber_user.p12"))
COT_PKCS12_TRUSTSTORE = os.getenv("COT_PKCS12_TRUSTSTORE", str(BASE_DIR / "truststore-root.p12"))
COT_PKCS12_PASSWORD = os.getenv("COT_PKCS12_PASSWORD", "atakatak")
COT_TLS_DIR = os.getenv("COT_TLS_DIR", str(TRACKING_DIR / "tls"))
PYTAK_TLS_CA_CERT = os.getenv("PYTAK_TLS_CA_CERT", "")
PYTAK_TLS_CLIENT_CERT = os.getenv("PYTAK_TLS_CLIENT_CERT", "")
PYTAK_TLS_CLIENT_KEY = os.getenv("PYTAK_TLS_CLIENT_KEY", "")
PYTAK_TLS_DONT_CHECK_HOSTNAME = os.getenv("PYTAK_TLS_DONT_CHECK_HOSTNAME", "")
COT_TLS_SERVER_NAME = os.getenv("COT_TLS_SERVER_NAME", "takserver")

# ---- Tracking behaviour ----
# Consider a device stale after this many minutes without a fresh point.
STALE_MINUTES = 15

# Round coordinates for CSV/reporting
LAT_LON_DECIMALS = 6

# ---- CSV header used by the append-only customer log ----
CSV_HEADER = [
    "device_id", "utc_time", "local_date", "local_time",
    "lat", "lon", "alt_m", "alt_ft",
    "temp_k", "pressure_hpa", "status", "raw"
]
