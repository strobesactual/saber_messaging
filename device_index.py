# device_index.py
from __future__ import annotations
import os, threading, csv
from collections import deque
from datetime import datetime, timezone

# --- Tunables (override via env) ---
STALE_MINUTES = int(os.getenv("STALE_MINUTES", "15"))
ALT_FLIGHT_THRESHOLD = float(os.getenv("ALT_FLIGHT_THRESHOLD", "50"))  # meters
MAX_WARM_ROWS = int(os.getenv("TRACKER_WARM_ROWS", "5000"))

_idx = {}                 # device_id -> record dict
_lock = threading.RLock() # protect _idx if you scale workers

def _safe_float(x):
    try:
        return round(float(x), 6)
    except Exception:
        return ""

def _compute_status(rec: dict, now_utc: datetime) -> str:
    last_seen = rec.get("last_seen_utc", now_utc)
    age_min = (now_utc - last_seen).total_seconds() / 60.0
    alt_m = float(rec.get("alt_m") or 0.0)
    if age_min > STALE_MINUTES:
        return "STALE"
    if alt_m >= ALT_FLIGHT_THRESHOLD:
        return "IN_FLIGHT"
    return "ON_GROUND"

def _parse_last_seen(local_date: str, local_time: str) -> datetime:
    # CSV stores "DD Mon YY" + "HH:MM:SS" as local wall clock; keep as UTC-ish for ordering
    try:
        dt = datetime.strptime(f"{local_date} {local_time}", "%d %b %y %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def warm_start(csv_path: str, max_rows: int = MAX_WARM_ROWS):
    """Load the last N rows from CSV to prefill the in-memory device index."""
    try:
        with open(csv_path, "r", newline="") as f:
            dq = deque(f, maxlen=max_rows)
        reader = csv.DictReader(dq)
        now_utc = datetime.now(timezone.utc)
        with _lock:
            for row in reader:
                did = (row.get("Device ID") or "").strip()
                if not did:
                    continue
                rec = {
                    "device_id": did,
                    "utc_time": row.get("UTC Time", ""),
                    "local_date": row.get("Local Date", ""),
                    "local_time": row.get("Local Time", ""),
                    "lat": _safe_float(row.get("Latitude")),
                    "lon": _safe_float(row.get("Longitude")),
                    "alt_m": _safe_float(row.get("Altitude (m)")),
                    "alt_ft": _safe_float(row.get("Altitude (ft)")),
                    "temp_k": _safe_float(row.get("Temp (K)")),
                    "pressure_hpa": _safe_float(row.get("Pressure (hPa)")),
                    "raw": row.get("Raw Message", ""),
                    "last_seen_utc": _parse_last_seen(row.get("Local Date",""), row.get("Local Time","")),
                    "first_seen_utc": now_utc,  # unknown from CSV; initialize
                    "last_position_utc": None,
                    "message_count": 0,
                    "questionable_data": False,  # warm-started rows are not questionable
                }
                # if position is present, set last_position_utc
                if isinstance(rec["lat"], (int, float)) and isinstance(rec["lon"], (int, float)):
                    rec["last_position_utc"] = rec["last_seen_utc"]
                rec["status"] = _compute_status(rec, now_utc)
                _idx[did] = rec
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[tracker] warm_start error: {e}")

def update(record: dict):
    """Update the index with a freshly-decoded record.
       If new data lacks lat/lon/alt/temp/pressure, preserve prior values
       and set 'questionable_data' to True for this record.
    """
    now_utc = datetime.now(timezone.utc)
    did = (record.get("device_id") or "").strip()
    if not did:
        return

    # Normalize incoming
    new_rec = {
        "device_id": did,
        "utc_time": record.get("utc_time", ""),
        "local_date": record.get("local_date", ""),
        "local_time": record.get("local_time", ""),
        "lat": _safe_float(record.get("lat")),
        "lon": _safe_float(record.get("lon")),
        "alt_m": _safe_float(record.get("alt_m")),
        "alt_ft": _safe_float(record.get("alt_ft")),
        "temp_k": _safe_float(record.get("temp_k")),
        "pressure_hpa": _safe_float(record.get("pressure_hpa")),
        "raw": record.get("raw", ""),
        "last_seen_utc": now_utc,
    }

    prior = _idx.get(did, {})
    carried = []
    # Sticky merge for key sensor fields
    for k in ("lat", "lon", "alt_m", "alt_ft", "temp_k", "pressure_hpa"):
        if new_rec[k] == "" and prior.get(k, "") != "":
            new_rec[k] = prior[k]
            carried.append(k)

    # Questionable flag if we had to carry anything
    new_rec["questionable_data"] = bool(carried)
    if carried:
        new_rec["carried_fields"] = carried

    # First/last position timestamps
    new_rec["first_seen_utc"] = prior.get("first_seen_utc", now_utc)
    pos_present = isinstance(new_rec["lat"], (int, float)) and isinstance(new_rec["lon"], (int, float))
    new_rec["last_position_utc"] = now_utc if pos_present else prior.get("last_position_utc")

    # Message count
    new_rec["message_count"] = int(prior.get("message_count", 0)) + 1

    # Status (uses altitude if available)
    alt_m_val = (new_rec["alt_m"] if isinstance(new_rec["alt_m"], (int, float))
                 else (prior.get("alt_m") if isinstance(prior.get("alt_m"), (int, float)) else 0.0))
    new_rec["status"] = _compute_status({"alt_m": alt_m_val, "last_seen_utc": now_utc}, now_utc)

    _idx[did] = new_rec

def get_all() -> list[dict]:
    """Newest-first list of devices, with position_age_min when available."""
    with _lock:
        out = []
        now_utc = datetime.now(timezone.utc)
        for v in _idx.values():
            d = dict(v)
            d["last_seen_utc"] = v["last_seen_utc"].isoformat()
            lpu = v.get("last_position_utc")
            if isinstance(lpu, datetime):
                d["last_position_utc"] = lpu.isoformat()
                d["position_age_min"] = round((now_utc - lpu).total_seconds() / 60.0, 2)
            out.append(d)
        return sorted(out, key=lambda r: r.get("last_seen_utc",""), reverse=True)

def get_one(device_id: str) -> dict | None:
    with _lock:
        v = _idx.get(device_id)
        if not v:
            return None
        d = dict(v)
        d["last_seen_utc"] = v["last_seen_utc"].isoformat()
        lpu = v.get("last_position_utc")
        if isinstance(lpu, datetime):
            d["last_position_utc"] = lpu.isoformat()
            d["position_age_min"] = round((datetime.now(timezone.utc) - lpu).total_seconds() / 60.0, 2)
        return d
