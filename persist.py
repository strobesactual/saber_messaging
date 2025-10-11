# persist.py
import os, csv, json
from typing import Dict, Any

TRACKING_DIR = "tracking_data"
CSV_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.csv")
KML_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.kml")
GEOJSON_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.geojson")

CSV_HEADER = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
    "Temp (K)", "Pressure (hPa)", "Raw Message"
]

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

def ensure_directories():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        _seed_csv()
    if not os.path.exists(KML_FILE) or os.path.getsize(KML_FILE) == 0:
        _seed_kml()
    if not os.path.exists(GEOJSON_FILE) or os.path.getsize(GEOJSON_FILE) == 0:
        _seed_geojson()

def ensure_outputs_exist():
    ensure_directories()
    # CSV header present?
    try:
        with open(CSV_FILE, 'r', newline='') as f:
            first = f.readline()
            if not first or any(h not in first for h in ["Device ID", "UTC Time", "Local Date"]):
                _seed_csv()
    except FileNotFoundError:
        _seed_csv()
    # KML wrapper present?
    try:
        with open(KML_FILE, 'r+', encoding='utf-8') as f:
            content = f.read()
            if "</Document>" not in content:
                _seed_kml()
    except FileNotFoundError:
        _seed_kml()
    # GeoJSON valid?
    try:
        with open(GEOJSON_FILE, 'r+', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if not isinstance(data, dict) or data.get("type") != "FeatureCollection" or "features" not in data:
                    raise ValueError("bad geojson")
            except Exception:
                _seed_geojson()
    except FileNotFoundError:
        _seed_geojson()

def append_csv(data: Dict[str, Any]):
    ensure_outputs_exist()
    with open(CSV_FILE, mode='a', newline='') as f:
        csv.writer(f).writerow([
            data.get('device_id', ''),
            data.get('utc_time', ''),
            data.get('local_date', ''),
            data.get('local_time', ''),
            data.get('lat', ''),
            data.get('lon', ''),
            data.get('alt_m', ''),
            data.get('alt_ft', ''),
            data.get('temp_k', ''),
            data.get('pressure_hpa', ''),
            data.get('raw', '')
        ])

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
        "geometry": {"type": "Point", "coordinates": [data.get('lon', ''), data.get('lat', ''), data.get('alt_m', '')]},
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

