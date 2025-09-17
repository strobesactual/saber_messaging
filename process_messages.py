# 
# Kyberdyne Tracking Server
# 
# Purpose:
#   - Receive Globalstar messages (XML/hex or JSON/Base64)
#   - Decode lat/lon/alt/timestamps robustly
#   - Persist points to CSV, KML, and GeoJSON
#   - Serve read-only endpoints for downstream tools and quick live views
#
# Runtime:
#   - Flask app; safe under Gunicorn or standalone (dev)
#   - Creates data directory/files on import to work with WSGI servers


# === Standard Library / Third-Party Imports ==================================
from flask import Flask, request, jsonify, Response, send_file
import base64
import binascii
import os
import csv
import json
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
from typing import Optional
import pandas as pd

# === Flask App Init ==========================================================
app = Flask(__name__)

# === File/Path Configuration =================================================
# Data directory and output artifact paths (CSV, KML, GeoJSON)
TRACKING_DIR = "tracking_data"
CSV_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.csv")
KML_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.kml")
GEOJSON_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.geojson")

# =============================================================================
# Read-only Data Endpoints
# -----------------------------------------------------------------------------
# Serve the latest CSV, KML, and GeoJSON files for external systems to consume 
# No caching so clients always see the most recent writes
# Provide a simple HTML "live" table view for quick verification
# Accessible at: http://kyberdyne.ddns.net:5050/ (data.csv, data.kml, data.geojson, live)
# =============================================================================
@app.route("/data.csv", methods=["GET"])
def get_csv():
    return send_file(CSV_FILE, mimetype="text/csv", max_age=0)

@app.route("/data.kml", methods=["GET"])
def get_kml():
    return send_file(KML_FILE, mimetype="application/vnd.google-earth.kml+xml", max_age=0)

@app.route("/data.geojson", methods=["GET"])
def get_geojson():
    return send_file(GEOJSON_FILE, mimetype="application/geo+json", max_age=0)

@app.route("/live", methods=["GET"])
def live_view():
    df = pd.read_csv(CSV_FILE)
    return df.to_html(classes="table table-striped", border=0)

# =============================================================================
# Filesystem Setup
# -----------------------------------------------------------------------------
# Ensure the data directory and seed output files exist at import-time so that
# the app works when run under Gunicorn (no reliance on __main__ branch).
# =============================================================================
def ensure_directories():
    os.makedirs(TRACKING_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "Device ID", "UTC Time", "Local Date", "Local Time", "Latitude", "Longitude", 
                "Altitude (m)", "Altitude (ft)", "Raw Message"
            ])
    if not os.path.exists(KML_FILE):
        with open(KML_FILE, 'w') as f:
            f.write("""<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document></Document></kml>""")
    if not os.path.exists(GEOJSON_FILE):
        with open(GEOJSON_FILE, 'w') as f:
            json.dump({"type": "FeatureCollection", "features": []}, f)

# Make sure paths exist even under Gnuicorn (not just __main__)
ensure_directories()

# =============================================================================
# Decoding Helpers (Field Selection/Normalization)
# -----------------------------------------------------------------------------
# _choose_latlon: try multiple encodings (endianness + offset), prefer plausible
#                 values and non-extreme latitudes; return rounded values + mode
# _choose_alt:    handle endianness/overflow-ish payloads; clamp absurd values
# _choose_timestamp: prefer plausible epoch seconds; derive local time from lon
# =============================================================================
def _choose_latlon(lat_bytes: bytes, lon_bytes: bytes):
    # Prefer signed microdegrees (little-endian) first, then others. Avoid extreme latitudes when multiple are valid.
    cands = [
        ("microdeg_le",
         int.from_bytes(lat_bytes, "little", signed=True)/1e7,
         int.from_bytes(lon_bytes, "little", signed=True)/1e7),
        ("microdeg_be",
         int.from_bytes(lat_bytes, "big", signed=True)/1e7,
         int.from_bytes(lon_bytes, "big", signed=True)/1e7),
        ("offset_le",
         int.from_bytes(lat_bytes, "little")/1e6 - 90,
         int.from_bytes(lon_bytes, "little")/1e6 - 180),
        ("offset_be",
         int.from_bytes(lat_bytes, "big")/1e6 - 90,
         int.from_bytes(lon_bytes, "big")/1e6 - 180),
    ]

    valid = [(m, lat, lon) for (m, lat, lon) in cands
             if -90 <= lat <= 90 and -180 <= lon <= 180]
    if not valid:
        raise ValueError("Decode produced no valid lat/lon")
    
    # Prefer non-extreme latitudes; then smallest absolute latitude as tie-breaker
    valid.sort(key=lambda t: (abs(t[1]) > 70, abs(t[1])))
    mode, lat, lon = valid[0]
    return mode, round(lat, 6), round(lon, 6)

def _choose_alt(alt_bytes: bytes):
    #Try little/big-endian and simple scaling; clamp absurd values.
    alt_le = int.from_bytes(alt_bytes, "little")
    alt_be = int.from_bytes(alt_bytes, "big")
    alt = alt_le
    if alt > 60000:              # >60 km? try shift/scale
        alt = alt_le >> 8
    if alt > 60000:
        alt = alt_be >> 8
    if alt > 60000:
        alt = 0
    return int(alt)

def _choose_timestamp(ts_bytes: bytes, lon_for_tz: Optional[float]):
    # Prefer plausible epoch seconds; fallback to now (UTC). Derive coarse local time from lon.
    now = datetime.now(timezone.utc)

    def plausible(sec: int):
        return 1420070400 <= sec <= int(now.timestamp()) + 7*86400  # 2015-01-01 .. now+7d

    ts_be = int.from_bytes(ts_bytes, "big")
    ts_le = int.from_bytes(ts_bytes, "little")

    if plausible(ts_be):
        ts = ts_be
    elif plausible(ts_le):
        ts = ts_le
    else:
        ts = int(now.timestamp())

    utc_time = datetime.fromtimestamp(ts, tz=timezone.utc)

    try:
        offset_hours = round((lon_for_tz or 0) / 15)
        if offset_hours < -12 or offset_hours > 14:
            offset_hours = 0
        local_time = utc_time.astimezone(timezone(timedelta(hours=offset_hours)))
    except Exception:
        local_time = utc_time

    return utc_time, local_time

# =============================================================================
# Decoders (Hex path for XML; Base64 path for JSON)
# -----------------------------------------------------------------------------
# _decode_from_hexstring: accept '0x...' or bare hex; parse fixed layout and
#                         return normalized dict for writers.
# decode_message:         accept Base64; parse same layout; return dict.
# Layout assumed:
#   [0:2]=hdr, [2:6]=lat(4B), [6:10]=lon(4B), [10:14]=alt(4B), [14:18]=time(4B)
# =============================================================================
def _decode_from_hexstring(hex_text: str):
    """
    Accepts '0xC0...' or 'C0...' hex; returns dict for CSV/KML/GeoJSON.
    Layout assumed: [0:2]=hdr, [2:6]=lat(4B), [6:10]=lon(4B), [10:14]=alt(4B), [14:18]=time(4B)
    """
    try:
        cleaned = hex_text.strip()
        if cleaned.lower().startswith("0x"):
            cleaned = cleaned[2:]
        raw = binascii.unhexlify(cleaned)  # validates hex

        if len(raw) < 18:
            raise ValueError(f"payload too short: {len(raw)} bytes")

        header = raw[0:2]
        lat_b  = raw[2:6]
        lon_b  = raw[6:10]
        alt_b  = raw[10:14]
        ts_b   = raw[14:18]

        mode, lat, lon = _choose_latlon(lat_b, lon_b)
        alt_m = _choose_alt(alt_b)
        utc_time, local_time = _choose_timestamp(ts_b, lon)

        return {
            "device_id": header.hex()[2:4],  # preserves your original behavior
            "lat": lat,
            "lon": lon,
            "alt_m": alt_m,
            "alt_ft": round(alt_m * 3.28084, 2),
            "utc_time": utc_time.strftime("%H:%M:%S"),
            "local_date": local_time.strftime("%d %b %y"),
            "local_time": local_time.strftime("%H:%M:%S"),
            "raw": "hex:" + cleaned + f" (mode={mode})"
        }
    except Exception as e:
        raise ValueError(f"Decode error (hex): {e}")

def decode_message(payload_b64):
    """
    Base64 decoder for /message JSON. Uses the same robust logic as hex path.
    """
    try:
        raw = base64.b64decode(payload_b64)
        if len(raw) < 18:
            raise ValueError(f"payload too short: {len(raw)} bytes")

        header = raw[0:2]
        lat_b  = raw[2:6]
        lon_b  = raw[6:10]
        alt_b  = raw[10:14]
        ts_b   = raw[14:18]

        mode, lat, lon = _choose_latlon(lat_b, lon_b)
        alt_m = _choose_alt(alt_b)
        utc_time, local_time = _choose_timestamp(ts_b, lon)

        return {
            "device_id": header.hex()[2:4],
            "lat": lat,
            "lon": lon,
            "alt_m": alt_m,
            "alt_ft": round(alt_m * 3.28084, 2),
            "utc_time": utc_time.strftime("%H:%M:%S"),
            "local_date": local_time.strftime("%d %b %y"),
            "local_time": local_time.strftime("%H:%M:%S"),
            "raw": payload_b64 + f" (mode={mode})"
        }
    except Exception as e:
        raise ValueError(f"Decode error: {e}")

# =============================================================================
# Writers (CSV / KML / GeoJSON)
# -----------------------------------------------------------------------------
# append_csv:     append a single decoded record to CSV
# append_kml:     append a Placemark to the KML <Document>
# append_geojson: append a Feature to the FeatureCollection
# _store_point:   convenience wrapper to write to all outputs
# =============================================================================
def append_csv(data):
    with open(CSV_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            data['device_id'], data['utc_time'], data['local_date'], data['local_time'], data['lat'], data['lon'], 
            data['alt_m'], data['alt_ft'], data['raw']
        ])

def append_kml(data):
    placemark = f"""
    <Placemark>
      <name>{data['device_id']}</name>
      <description>Alt: {data['alt_m']}m / {data['alt_ft']}ft\nUTC: {data['utc_time']}\nRaw: {data['raw']}</description>
      <Point><coordinates>{data['lon']},{data['lat']},{data['alt_m']}</coordinates></Point>
    </Placemark>
    """
    with open(KML_FILE, 'r+', encoding='utf-8') as f:
        content = f.read()
        updated = content.replace("</Document>", f"{placemark}</Document>")
        f.seek(0)
        f.write(updated)
        f.truncate()

def append_geojson(data):
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [data['lon'], data['lat'], data['alt_m']]},
        "properties": {
            "device_id": data['device_id'],
            "alt_ft": data['alt_ft'],
            "utc_time": data['utc_time'],
            "local_date": data['local_date'],
            "local_time": data['local_time'],
            "raw": data['raw']
        }
    }
    with open(GEOJSON_FILE, 'r+', encoding='utf-8') as f:
        content = json.load(f)
        content['features'].append(feature)
        f.seek(0)
        json.dump(content, f, indent=2)
        f.truncate()

def _store_point(data):
    append_csv(data)
    append_kml(data)
    append_geojson(data)

# =============================================================================
# Health Endpoint
# -----------------------------------------------------------------------------
# Lightweight liveness probe for orchestration / monitoring.
# =============================================================================
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

# =============================================================================
# Globalstar XML Ingest (POST /)
# -----------------------------------------------------------------------------
# Accepts Globalstar XML (text/xml) containing one or more <stuMessage> payloads.
# - Only 'hex' payloads are supported here.
# - Responds with the required <stuResponseMsg> XML acknowledging success/failure.
# GET / returns a simple service banner for quick checks.
# =============================================================================
def _xml_response(correlation_id: str, state="pass", state_message="Store OK"):
    ts = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S GMT")
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<stuResponseMsg xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:noNamespaceSchemaLocation="http://cody.glpconnect.com/XSD/StuResponse_Rev1_0.xsd"
  deliveryTimeStamp="{ts}"
  correlationID="{correlation_id}">
  <state>{state}</state>
  <stateMessage>{state_message}</stateMessage>
</stuResponseMsg>"""
    return Response(xml, mimetype="text/xml", status=200)

@app.route("/", methods=["GET", "POST"])
def root():
    # Health check for GET
    if request.method == "GET":
        return "Kyberdyne Tracking Server Active"

    # POST: Expect Globalstar XML with text/xml
    try:
        raw_body = request.data.decode("utf-8", errors="ignore")
        # Parse incoming XML <stuMessages ... messageID="...">
        root_el = ET.fromstring(raw_body)
        corr_id = root_el.attrib.get("messageID", "unknown")

        processed = 0
        for child in root_el:
            if child.tag.endswith("stuMessage"):
                payload_el = child.find("./payload")
                if payload_el is None or not (payload_el.text or "").strip():
                    continue
                encoding = (payload_el.attrib.get("encoding") or "").lower()
                payload_text = payload_el.text.strip()

                if encoding == "hex":
                    decoded = _decode_from_hexstring(payload_text)
                    _store_point(decoded)
                    processed += 1
                else:
                    raise ValueError(f"Unsupported payload encoding: {encoding}")

        msg = f"{processed} messages received and stored successfully" if processed else "No stuMessage payloads found"
        return _xml_response(corr_id, state="pass", state_message=msg)

    except Exception as e:
        # Return 200 with fail state per ICD when something goes wrong
        return _xml_response("error", state="fail", state_message=str(e))

# =============================================================================
# JSON Ingest (POST /message)
# -----------------------------------------------------------------------------
# Accepts JSON payloads with Base64-encoded message body. Decodes and stores
# using the same normalization logic as the XML/hex path.
# Example body: {"payload": "BASE64_STRING"}
# =============================================================================
@app.route('/message', methods=['POST'])
def receive_message():
    try:
        payload = request.json.get('payload')
        if not payload:
            return jsonify({"error": "Missing payload"}), 400
        decoded = decode_message(payload)
        _store_point(decoded)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# =============================================================================
# Dev Entry Point (Standalone Only)
# -----------------------------------------------------------------------------
# For local testing. In production, run under Gunicorn (example):
#   ubuntu@HOST:~/globalstar_receiver$ ./venv/bin/gunicorn -w 1 -b 0.0.0.0:5050 process_messages:app
# =============================================================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050)
