# ---------------------------------------------------------------------------
# Persists each observation:
#   1) Upsert into SQLite table device_latest (authoritative "latest" state)
#   2) Append a full-flight CSV log (all messages)  [toggle via CSV_LOG_ENABLED]
#   3) Regenerate GeoJSON + KML snapshots of latest positions [toggles]
# ---------------------------------------------------------------------------

from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

# ----------------------------
# Config (with safe fallbacks)
# ----------------------------
try:
    from . import config as cfg  # your app/config.py
except Exception:
    cfg = None  # fall back to defaults below

TRACKING_DIR = Path(getattr(cfg, "TRACKING_DIR", Path("tracking_data")))
DB_PATH       = Path(getattr(cfg, "DB_PATH",        TRACKING_DIR / "device_latest.db"))
CSV_LOG_PATH  = Path(getattr(cfg, "CSV_LOG_PATH",   TRACKING_DIR / "kyberdyne_tracking.csv"))
GEOJSON_PATH  = Path(getattr(cfg, "GEOJSON_PATH",   TRACKING_DIR / "kyberdyne_tracking.geojson"))
KML_PATH      = Path(getattr(cfg, "KML_PATH",       TRACKING_DIR / "kyberdyne_tracking.kml"))

CSV_LOG_ENABLED         = bool(getattr(cfg, "CSV_LOG_ENABLED", True))
GEOJSON_EXPORT_ENABLED  = bool(getattr(cfg, "GEOJSON_EXPORT_ENABLED", True))
KML_EXPORT_ENABLED      = bool(getattr(cfg, "KML_EXPORT_ENABLED", True))

TRACKING_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# SQLite schema / helpers
# ----------------------------
DDL = """
CREATE TABLE IF NOT EXISTS device_latest (
  device_id           TEXT PRIMARY KEY,
  lat                 REAL,
  lon                 REAL,
  alt_m               REAL,
  alt_ft              REAL,
  temp_k              REAL,
  pressure_hpa        REAL,
  status              TEXT,
  questionable_data   INTEGER DEFAULT 0,
  utc_time            TEXT,
  local_date          TEXT,
  local_time          TEXT,
  raw                 TEXT,
  message_count       INTEGER DEFAULT 0,
  first_seen_utc      TEXT,
  last_position_utc   TEXT
);
"""

UPSERT = """
INSERT INTO device_latest (
  device_id, lat, lon, alt_m, alt_ft, temp_k, pressure_hpa, status,
  questionable_data, utc_time, local_date, local_time, raw,
  message_count, first_seen_utc, last_position_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(device_id) DO UPDATE SET
  lat=excluded.lat,
  lon=excluded.lon,
  alt_m=excluded.alt_m,
  alt_ft=excluded.alt_ft,
  temp_k=excluded.temp_k,
  pressure_hpa=excluded.pressure_hpa,
  status=excluded.status,
  questionable_data=excluded.questionable_data,
  utc_time=excluded.utc_time,
  local_date=excluded.local_date,
  local_time=excluded.local_time,
  raw=excluded.raw,
  message_count=device_latest.message_count + 1,
  last_position_utc=excluded.last_position_utc
;
"""

SELECT_GOOD = """
SELECT device_id, lat, lon, alt_m, alt_ft, temp_k, pressure_hpa, status,
       questionable_data, utc_time, local_date, local_time, raw,
       message_count, first_seen_utc, last_position_utc
FROM device_latest
WHERE lat IS NOT NULL AND lon IS NOT NULL AND questionable_data=0
"""

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute(DDL)
    return con

# ----------------------------
# CSV log (append-only)
# ----------------------------
CSV_FIELDS = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
    "Temp (K)", "Pressure (hPa)", "Raw Message"
]

def _append_csv(ob: Dict[str, Any]) -> None:
    if not CSV_LOG_ENABLED:
        return
    CSV_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fresh = not CSV_LOG_PATH.exists()
    with CSV_LOG_PATH.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if fresh:
            w.writeheader()
        row = {
            "Device ID": ob.get("device_id"),
            "UTC Time": ob.get("utc_time"),
            "Local Date": ob.get("local_date"),
            "Local Time": ob.get("local_time"),
            "Latitude": ob.get("lat"),
            "Longitude": ob.get("lon"),
            "Altitude (m)": ob.get("alt_m"),
            "Altitude (ft)": ob.get("alt_ft"),
            "Temp (K)": ob.get("temp_k"),
            "Pressure (hPa)": ob.get("pressure_hpa"),
            "Raw Message": ob.get("raw"),
        }
        w.writerow(row)

# ----------------------------
# GeoJSON + KML snapshots
# ----------------------------
def _row_to_feature(row: Tuple) -> Dict[str, Any]:
    (device_id, lat, lon, alt_m, alt_ft, temp_k, pressure_hpa, status,
     questionable_data, utc_time, local_date, local_time, raw,
     message_count, first_seen_utc, last_position_utc) = row

    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "device_id": device_id,
            "alt_m": alt_m,
            "alt_ft": alt_ft,
            "temp_k": temp_k,
            "pressure_hpa": pressure_hpa,
            "status": status,
            "questionable_data": bool(questionable_data),
            "utc_time": utc_time,
            "local_date": local_date,
            "local_time": local_time,
            "message_count": message_count,
            "first_seen_utc": first_seen_utc,
            "last_position_utc": last_position_utc,
            "raw": raw,
        },
    }

def _write_geojson(rows: Iterable[Tuple]) -> None:
    if not GEOJSON_EXPORT_ENABLED:
        return
    features = [_row_to_feature(r) for r in rows]
    GEOJSON_PATH.write_text(json.dumps({"type": "FeatureCollection","features": features}, indent=2))

def _write_kml(rows: Iterable[Tuple]) -> None:
    if not KML_EXPORT_ENABLED:
        return
    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n'
        "  <Document>\n"
        "    <name>Saber Devices (Latest)</name>\n"
    )
    footer = "  </Document>\n</kml>\n"

    placemarks: List[str] = []
    for r in rows:
        (device_id, lat, lon, alt_m, alt_ft, temp_k, pressure_hpa, status,
         questionable_data, utc_time, local_date, local_time, raw,
         message_count, first_seen_utc, last_position_utc) = r
        desc = (
            f"status={status}, alt_m={alt_m}, temp_k={temp_k}, "
            f"pressure_hpa={pressure_hpa}, last_position_utc={last_position_utc}, "
            f"msg_count={message_count}"
        )
        placemarks.append(
            "    <Placemark>\n"
            f"      <name>{device_id}</name>\n"
            f"      <description>{desc}</description>\n"
            "      <Point>\n"
            f"        <coordinates>{lon},{lat},{alt_m if alt_m is not None else 0}</coordinates>\n"
            "      </Point>\n"
            "    </Placemark>\n"
        )

    KML_PATH.write_text(header + "".join(placemarks) + footer)

# ----------------------------
# Public API
# ----------------------------
def _fallback_status(ob: Dict[str, Any]) -> str:
    try:
        return "IN_FLIGHT" if float(ob.get("alt_m") or 0.0) > 50.0 else "ON_GROUND"
    except Exception:
        return "UNKNOWN"

def _fallback_questionable(ob: Dict[str, Any]) -> int:
    try:
        lat = float(ob.get("lat"))
        lon = float(ob.get("lon"))
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return 1
        if abs(lat) < 1e-9 and abs(lon) < 1e-9:
            return 1
        return 0
    except Exception:
        return 1

def record_observation(ob: Dict[str, Any]) -> None:
    """
    Upsert 'latest' row in SQLite, log CSV row, and refresh GeoJSON/KML.
    Accepts missing 'status' or 'questionable_data' and fills sensible defaults.
    """
    # 1) CSV log (append-only)
    _append_csv(ob)

    # Normalize missing flags
    status = ob.get("status") or _fallback_status(ob)
    qflag  = ob.get("questionable_data")
    qflag  = int(qflag) if isinstance(qflag, (int, bool)) else _fallback_questionable(ob)

    # 2) Upsert SQLite "latest"
    con = _ensure_db()
    try:
        first_seen = _utcnow_iso()
        con.execute(
            UPSERT,
            (
                ob.get("device_id"),
                ob.get("lat"),
                ob.get("lon"),
                ob.get("alt_m"),
                ob.get("alt_ft"),
                ob.get("temp_k"),
                ob.get("pressure_hpa"),
                status,
                qflag,
                ob.get("utc_time"),
                ob.get("local_date"),
                ob.get("local_time"),
                ob.get("raw"),
                first_seen,
                ob.get("last_position_utc"),
            ),
        )
        con.commit()

        # 3) Regenerate GeoJSON and KML snapshots from the DB "good" rows
        cur = con.execute(SELECT_GOOD)
        rows = cur.fetchall()
        _write_geojson(rows)
        _write_kml(rows)
    finally:
        con.close()
