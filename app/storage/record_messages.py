# app/storage/record_messages.py
# Functions to append tracking data to CSV, KML, GeoJSON files.
# Also maintain a per-device "latest known" CSV file.


import os, csv, json
from typing import Dict, Any

TRACKING_DIR = "tracking_data"
CSV_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.csv")
KML_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.kml")
GEOJSON_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.geojson")
LATEST_CSV = os.path.join(TRACKING_DIR, "device_latest.csv")  # per-device last known location

CSV_HEADER = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
    "Temp (K)", "Pressure (hPa)", "Raw Message", "Status"
]
LATEST_HEADER = CSV_HEADER[:]  # same layout for simplicity

def _seed_csv():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    with open(CSV_FILE, mode='w', newline='') as f:
        csv.writer(f).writerow(CSV_HEADER)

def _seed_kml():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    with open(KML_FILE, 'w', encoding='utf-8') as f:
        f.write(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2"><Document></Document></kml>'
        )

def _seed_geojson():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    with open(GEOJSON_FILE, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": []}, f)

def _seed_latest():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    with open(LATEST_CSV, mode='w', newline='') as f:
        csv.writer(f).writerow(LATEST_HEADER)

def ensure_directories():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        _seed_csv()
    if not os.path.exists(KML_FILE) or os.path.getsize(KML_FILE) == 0:
        _seed_kml()
    if not os.path.exists(GEOJSON_FILE) or os.path.getsize(GEOJSON_FILE) == 0:
        _seed_geojson()
    if not os.path.exists(LATEST_CSV) or os.path.getsize(LATEST_CSV) == 0:
        _seed_latest()

def ensure_outputs_exist():
    ensure_directories()
    # CSV has header?
    try:
        with open(CSV_FILE, 'r', newline='') as f:
            first = f.readline()
            if not first or "Device ID" not in first:
                _seed_csv()
    except FileNotFoundError:
        _seed_csv()
    # KML well-formed?
    try:
        with open(KML_FILE, 'r', encoding='utf-8') as f:
            if "</Document>" not in f.read():
                _seed_kml()
    except FileNotFoundError:
        _seed_kml()
    # GeoJSON ok?
    try:
        with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
                _seed_geojson()
    except Exception:
        _seed_geojson()
    # Latest header?
    try:
        with open(LATEST_CSV, 'r', newline='') as f:
            first = f.readline()
            if not first or "Device ID" not in first:
                _seed_latest()
    except FileNotFoundError:
        _seed_latest()

def _row_from_data(d: Dict[str, Any]):
    return [
        d.get('device_id', ''),
        d.get('utc_time', ''),
        d.get('local_date', ''),
        d.get('local_time', ''),
        d.get('lat', ''),
        d.get('lon', ''),
        d.get('alt_m', ''),
        d.get('alt_ft', ''),
        d.get('temp_k', ''),
        d.get('pressure_hpa', ''),
        d.get('raw', ''),
    ]

def append_csv(data: Dict[str, Any]):
    ensure_outputs_exist()
    with open(CSV_FILE, mode='a', newline='') as f:
        csv.writer(f).writerow(_row_from_data(data))

def append_kml(data: Dict[str, Any]):
    ensure_outputs_exist()
    desc = (
        f"Alt: {data.get('alt_m','')}m / {data.get('alt_ft','')}ft"
        f"\\nTemp: {data.get('temp_k','')} K"
        f"\\nPressure: {data.get('pressure_hpa','')} hPa"
        f"\\nUTC: {data.get('utc_time','')}"
        f"\\nRaw: {data.get('raw','')}"
    )
    placemark = (
        f"\n    <Placemark>\n"
        f"      <name>{data.get('device_id','')}</name>\n"
        f"      <description>{desc}</description>\n"
        f"      <Point><coordinates>{data.get('lon','')},{data.get('lat','')},{data.get('alt_m','')}</coordinates></Point>\n"
        f"    </Placemark>\n"
    )
    # simple replace of closing tag
    with open(KML_FILE, 'r+', encoding='utf-8') as f:
        content = f.read()
        if "</Document>" not in content:
            _seed_kml()
            content = open(KML_FILE, 'r', encoding='utf-8').read()
        updated = content.replace("</Document>", f"{placemark}</Document>")
        f.seek(0); f.write(updated); f.truncate()

def append_geojson(data: Dict[str, Any]):
    ensure_outputs_exist()
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [data.get('lon', ''), data.get('lat', ''), data.get('alt_m', '')]
        },
        "properties": {
            "device_id": data.get('device_id', ''),
            "alt_ft": data.get('alt_ft', ''),
            "utc_time": data.get('utc_time', ''),
            "local_date": data.get('local_date', ''),
            "local_time": data.get('local_time', ''),
            "temp_k": data.get('temp_k', ''),
            "pressure_hpa": data.get('pressure_hpa', ''),
            "raw": data.get('raw', '')
        }
    }
    with open(GEOJSON_FILE, 'r+', encoding='utf-8') as f:
        try:
            content = json.load(f)
        except Exception:
            content = {"type": "FeatureCollection", "features": []}
        content.setdefault('features', []).append(feature)
        f.seek(0); json.dump(content, f, indent=2); f.truncate()

def write_latest_row(data: Dict[str, Any]):
    """
    Maintain a 'last-known' CSV (LATEST_CSV) with one row per device ID.
    If a field in this message is blank, we still keep previous non-blank from the last row.
    """
    ensure_outputs_exist()

    # Load existing into dict keyed by Device ID
    latest = {}
    try:
        with open(LATEST_CSV, 'r', newline='') as f:
            r = csv.DictReader(f)
            for row in r:
                latest[row.get("Device ID","")] = row
    except FileNotFoundError:
        pass

    did = str(data.get("device_id", "") or "")
    prev = latest.get(did, {h: "" for h in LATEST_HEADER})

    # Build merged row (prefer new non-empty values)
    merged = {
        "Device ID": did,
        "UTC Time": str(data.get("utc_time", "") or prev.get("UTC Time","")),
        "Local Date": str(data.get("local_date", "") or prev.get("Local Date","")),
        "Local Time": str(data.get("local_time", "") or prev.get("Local Time","")),
        "Latitude": data.get("lat", "") if str(data.get("lat","")) != "" else prev.get("Latitude",""),
        "Longitude": data.get("lon", "") if str(data.get("lon","")) != "" else prev.get("Longitude",""),
        "Altitude (m)": data.get("alt_m", "") if str(data.get("alt_m","")) != "" else prev.get("Altitude (m)",""),
        "Altitude (ft)": data.get("alt_ft", "") if str(data.get("alt_ft","")) != "" else prev.get("Altitude (ft)",""),
        "Temp (K)": data.get("temp_k", "") if str(data.get("temp_k","")) != "" else prev.get("Temp (K)",""),
        "Pressure (hPa)": data.get("pressure_hpa", "") if str(data.get("pressure_hpa","")) != "" else prev.get("Pressure (hPa)",""),
        "Raw Message": str(data.get("raw", "") or prev.get("Raw Message","")),
        "Status": data.get("status", "UNKNOWN")  # Add status column
    }

    latest[did] = merged

    # Rewrite file with header
    with open(LATEST_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=LATEST_HEADER)
        w.writeheader()
        for row in latest.values():
            w.writerow(row)