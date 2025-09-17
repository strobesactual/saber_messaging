# Kyberdyne Tracking Server

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
# Decoding (fixed Globalstar layout)
# -----------------------------------------------------------------------------
# Message format (17 bytes total):
#   [0]   = burn byte 0x02 (ignore)
#   [1:5] = latitude   (4B, big-endian)   -> lat = raw/1e5 - 90
#   [5:9] = longitude  (4B, big-endian)   -> lon = raw/1e5 - 180
#   [9:13]= altitude   (4B, big-endian)   -> alt_m = raw/100
#   [13:17]= time UTC  (4B, big-endian)   -> HHMMSSCC  (we use HH:MM:SS)
# =============================================================================

def _hhmmss_from_cc(raw_u32: int) -> str:
    """Convert HHMMSSCC integer to 'HH:MM:SS' string (drop hundredths)."""
    s = f"{raw_u32:08d}"[-8:]  # zero-pad and keep last 8 digits
    hh, mm, ss = s[0:2], s[2:4], s[4:6]
    return f"{hh}:{mm}:{ss}"

def _parse_fixed_payload(raw: bytes) -> dict:
    """
    Parse the fixed 0x02 + 4x4B layout directly from bytes.
    Returns normalized dict used by writers.
    """
    if len(raw) < 17:
        raise ValueError(f"payload too short: {len(raw)} bytes (need 17)")

    # Optional: sanity check burn byte
    # If it's not 0x02 we'll still proceed, but note it in 'raw' field.
    burn = raw[0]

    lat_u32 = int.from_bytes(raw[1:5],  byteorder="big", signed=False)
    lon_u32 = int.from_bytes(raw[5:9],  byteorder="big", signed=False)
    alt_u32 = int.from_bytes(raw[9:13], byteorder="big", signed=False)
    tim_u32 = int.from_bytes(raw[13:17],byteorder="big", signed=False)

    lat = round(lat_u32 / 1e5 - 90.0, 6)
    lon = round(lon_u32 / 1e5 - 180.0, 6)
    alt_m = int(alt_u32 / 100)  # integer meters per your table
    alt_ft = round(alt_m * 3.28084, 2)
    utc_hms = _hhmmss_from_cc(tim_u32)

    # Crude local-time offset from longitude (same approach you used before)
    try:
        offset_hours = round(lon / 15)
        if offset_hours < -12 or offset_hours > 14:
            offset_hours = 0
        utc_today = datetime.now(timezone.utc).replace(microsecond=0)
        utc_dt = utc_today.replace(hour=int(utc_hms[0:2]),
                                   minute=int(utc_hms[3:5]),
                                   second=int(utc_hms[6:8]))
        local_dt = utc_dt.astimezone(timezone(timedelta(hours=offset_hours)))
        local_date = local_dt.strftime("%d %b %y")
        local_time = local_dt.strftime("%H:%M:%S")
    except Exception:
        # Fallback—still provide UTC time-of-day
        local_date = datetime.now().strftime("%d %b %y")
        local_time = utc_hms

    return {
        "device_id": "",                 # not present in this message layout
        "lat": lat,
        "lon": lon,
        "alt_m": alt_m,
        "alt_ft": alt_ft,
        "utc_time": utc_hms,            # HH:MM:SS from HHMMSSCC
        "local_date": local_date,
        "local_time": local_time,
        "raw": f"bytes:{raw.hex()} (burn=0x{burn:02x}, fixed_layout_v1)"
    }

def _decode_from_hexstring(hex_text: str):
    """
    Accepts '0x...' or bare hex. Expected 34 hex chars (17 bytes):
      '02' + 32 chars for the 4 fields.
    """
    cleaned = (hex_text or "").strip()
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]
    if len(cleaned) < 34:
        raise ValueError(f"hex payload too short: {len(cleaned)} hex chars (need 34)")
    raw = binascii.unhexlify(cleaned[:34])  # enforce exactly one message
    return _parse_fixed_payload(raw)

def decode_message(payload_b64: str):
    """
    Base64 decoder for /message JSON. The Base64 must decode to:
      17 bytes: 0x02 + 4 fields (lat, lon, alt, time), all big-endian.
    """
    raw = base64.b64decode(payload_b64 or "")
    if len(raw) < 17:
        raise ValueError(f"base64 payload too short: {len(raw)} bytes (need 17)")
    # If devices batch multiple messages in one blob, only take the first 17B.
    return _parse_fixed_payload(raw[:17])


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
