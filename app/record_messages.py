# app/record_messages.py
# ---------------------------------------------------------------------------
# Responsibility:
#   - Persist each normalized observation.
#   - device_latest (SQLite) holds the authoritative "latest per device" row.
#   - CSV/GeoJSON/KML artifacts are maintained under tracking_data/.
# Data flow:
#   - Called by process_messages.record_observation(ob).
#   - UPSERTs device_latest, rolling up max_alt_m, then regenerates snapshots.
# ---------------------------------------------------------------------------

from __future__ import annotations

import csv
import json
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from . import status_utils

logger = logging.getLogger("ingest")
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
  callsign            TEXT,
  status              TEXT,
  lat                 REAL,
  lon                 REAL,
  alt_m               REAL,
  max_alt_m           REAL,
  alt_ft              REAL,
  temp_k              REAL,
  pressure_hpa        REAL,
  utc_time            TEXT,
  local_date          TEXT,
  local_time          TEXT,
  raw                 TEXT,
  message_count       INTEGER DEFAULT 0,
  first_seen_utc      TEXT,
  last_position_utc   TEXT,
  sr_num              INTEGER,
  flight_started      INTEGER DEFAULT 0,
  balloon_type        TEXT
);
"""

DDL_INGEST_SEEN = """
CREATE TABLE IF NOT EXISTS ingest_seen (
  correlation_id TEXT NOT NULL,
  raw            TEXT NOT NULL,
  seen_utc       TEXT NOT NULL,
  PRIMARY KEY (correlation_id, raw)
);
"""

DDL_INGEST_SEEN_RAW = """
CREATE TABLE IF NOT EXISTS ingest_seen_raw (
  device_id TEXT NOT NULL,
  raw       TEXT NOT NULL,
  seen_utc  TEXT NOT NULL,
  PRIMARY KEY (device_id, raw)
);
"""

UPSERT = """
INSERT INTO device_latest (
  device_id, callsign, status, lat, lon, alt_m, max_alt_m, alt_ft, temp_k, pressure_hpa,
  utc_time, local_date, local_time, raw,
  message_count, first_seen_utc, last_position_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(device_id) DO UPDATE SET
  callsign=excluded.callsign,
  status=excluded.status,
  lat=excluded.lat,
  lon=excluded.lon,
  alt_m=excluded.alt_m,
  max_alt_m=MAX(COALESCE(device_latest.max_alt_m,0), COALESCE(excluded.alt_m,0)),
  alt_ft=excluded.alt_ft,
  temp_k=excluded.temp_k,
  pressure_hpa=excluded.pressure_hpa,
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
       utc_time, local_date, local_time, raw,
       message_count, first_seen_utc, last_position_utc
FROM device_latest
WHERE lat IS NOT NULL AND lon IS NOT NULL
"""

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _logged_now() -> str:
    # Human-friendly stamp for CSV: DD MMM YY HH:MM:SS (UTC)
    return datetime.now(timezone.utc).strftime("%d %b %y %H:%M:%S")

def _parse_iso8601(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute(DDL)
    con.execute(DDL_INGEST_SEEN)
    con.execute(DDL_INGEST_SEEN_RAW)
    # Migrate columns if this is an older DB
    try:
        cur = con.execute("PRAGMA table_info(device_latest)")
        cols = {row[1] for row in cur.fetchall()}
        alters = []
        if "sr_num" not in cols:
            alters.append("ALTER TABLE device_latest ADD COLUMN sr_num INTEGER")
        if "callsign" not in cols:
            alters.append("ALTER TABLE device_latest ADD COLUMN callsign TEXT")
        if "flight_started" not in cols:
            alters.append("ALTER TABLE device_latest ADD COLUMN flight_started INTEGER DEFAULT 0")
        if "balloon_type" not in cols:
            alters.append("ALTER TABLE device_latest ADD COLUMN balloon_type TEXT")
        if "max_alt_m" not in cols:
            alters.append("ALTER TABLE device_latest ADD COLUMN max_alt_m REAL")
        for sql in alters:
            con.execute(sql)
        if alters:
            con.commit()
    except Exception:
        pass
    return con

# ----------------------------
# CSV log (append-only)
# ----------------------------
CSV_FIELDS = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
    "Temp (K)", "Pressure (hPa)", "Logged", "Raw Message"
]

def _append_csv(ob: Dict[str, Any]) -> None:
    if not CSV_LOG_ENABLED:
        return
    # De-dupe CSV on (correlation_id, raw) if both present
    cid = str(ob.get("correlation_id") or "").strip()
    raw_hex = str(ob.get("raw") or "").strip()
    device_id = str(ob.get("device_id") or "").strip()

    con = None
    try:
        if cid and raw_hex:
            con = _ensure_db()
            before = con.total_changes
            con.execute(
                "INSERT OR IGNORE INTO ingest_seen (correlation_id, raw, seen_utc) VALUES (?, ?, ?)",
                (cid, raw_hex, _utcnow_iso()),
            )
            con.commit()
            if con.total_changes == before:
                # Already seen — skip CSV append
                return
        # De-dupe CSV on (device_id, raw) for retransmits without correlation_id
        if device_id and raw_hex:
            if con is None:
                con = _ensure_db()
            before = con.total_changes
            con.execute(
                "INSERT OR IGNORE INTO ingest_seen_raw (device_id, raw, seen_utc) VALUES (?, ?, ?)",
                (device_id, raw_hex, _utcnow_iso()),
            )
            con.commit()
            if con.total_changes == before:
                return
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
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
            "Logged": _logged_now(),
            "Raw Message": ob.get("raw"),
        }
        w.writerow(row)

# ----------------------------
# GeoJSON + KML snapshots
# ----------------------------
def _row_to_feature(row: Tuple) -> Dict[str, Any]:
    (device_id, lat, lon, alt_m, alt_ft, temp_k, pressure_hpa, status,
     utc_time, local_date, local_time, raw,
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

def record_observation(ob: Dict[str, Any]) -> bool:
    """
    Upsert 'latest' row in SQLite, log CSV row, and refresh GeoJSON/KML.
    Accepts missing 'status' or 'questionable_data' and fills sensible defaults.
    Returns True if the SQLite/KML/GeoJSON snapshots were updated, False if the
    observation was older/equal to the stored latest for that device.
    """
    raw_hex = str(ob.get("raw") or "").strip().lower()
    if not raw_hex:
        logger.info("ignoring device=%s missing raw payload", ob.get("device_id"))
        return False
    if raw_hex.startswith("00"):
        logger.info("ignoring device=%s raw startswith 0x00", ob.get("device_id"))
        return False
    if not raw_hex.startswith("02"):
        logger.info("ignoring device=%s raw missing 0x02 header: %s", ob.get("device_id"), raw_hex[:8])
        return False

    # 1) CSV log (append-only)
    _append_csv(ob)

    new_last_seen = ob.get("last_position_utc") or _utcnow_iso()

    # Normalize
    status = ob.get("status") or _fallback_status(ob)
    # Populate callsign from last 3 digits if not provided
    callsign = ob.get("callsign")
    if not callsign and ob.get("device_id"):
        tail = str(ob["device_id"])[-3:]
        callsign = f"SR{tail}" if tail.isdigit() else "SR00"

    # 2) Upsert SQLite "latest"
    con = _ensure_db()
    try:
        first_seen = _utcnow_iso()
        # compute visual_status & max_alt rollup within UPSERT
        # we need current persisted flight_started to compute visual status robustly
        cur = con.execute(
            "SELECT flight_started, max_alt_m, last_position_utc FROM device_latest WHERE device_id=?",
            (ob.get("device_id"),),
        )
        row = cur.fetchone()
        flight_started = bool(row[0]) if row else False
        prior_last_seen = row[2] if row and len(row) > 2 else None

        prior_dt = _parse_iso8601(prior_last_seen)
        new_dt = _parse_iso8601(new_last_seen)
        if prior_dt and new_dt and new_dt <= prior_dt:
            # Older or duplicate message by timestamp — keep existing latest.
            return False

        # derive visual_status using shared util (AGL-aware if SRTM is present)
        visual_status = status_utils.compute_visual_status(
            lat=(ob.get("lat") if ob.get("lat") not in ("", None) else None),
            lon=(ob.get("lon") if ob.get("lon") not in ("", None) else None),
            alt_m=(ob.get("alt_m") if ob.get("alt_m") not in ("", None) else None),
            last_position_utc=ob.get("last_position_utc"),
            flight_started=flight_started,
        )

        con.execute(
            UPSERT,
            (
                ob.get("device_id"),
                callsign,
                visual_status,
                ob.get("lat"),
                ob.get("lon"),
                ob.get("alt_m"),
                ob.get("alt_m"),  # seed max_alt_m with current alt_m; UPSERT does MAX()
                ob.get("alt_ft"),
                ob.get("temp_k"),
                ob.get("pressure_hpa"),
                ob.get("utc_time"),
                ob.get("local_date"),
                ob.get("local_time"),
                ob.get("raw"),
                first_seen,
                new_last_seen,
            ),
        )
        con.commit()

        # 3) Regenerate GeoJSON and KML snapshots from the DB "good" rows
        cur = con.execute(SELECT_GOOD)
        rows = cur.fetchall()
        _write_geojson(rows)
        _write_kml(rows)

        return True
    finally:
        con.close()
