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
#   - Creates/repairs data files both at startup and before each write

# === Standard Library / Third-Party Imports ==================================
from flask import Flask, request, jsonify, Response, send_file
import base64
import binascii
import os
import csv
import json
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import pandas as pd

# === Flask App Init ==========================================================
app = Flask(__name__)

# === File/Path Configuration =================================================
TRACKING_DIR = "tracking_data"
CSV_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.csv")
KML_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.kml")
GEOJSON_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.geojson")

CSV_HEADER = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)", "Raw Message"
]

# =============================================================================
# Filesystem Setup / Self-Healing
# -----------------------------------------------------------------------------
def _seed_csv():
    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)

def _seed_kml():
    with open(KML_FILE, 'w', encoding='utf-8') as f:
        f.write(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2"><Document></Document></kml>'
        )

def _seed_geojson():
    with open(GEOJSON_FILE, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection", "features": []}, f)

def ensure_directories():
    """Create data directory and seed files if missing or empty."""
    os.makedirs(TRACKING_DIR, exist_ok=True)

    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        _seed_csv()

    if not os.path.exists(KML_FILE) or os.path.getsize(KML_FILE) == 0:
        _seed_kml()

    if not os.path.exists(GEOJSON_FILE) or os.path.getsize(GEOJSON_FILE) == 0:
        _seed_geojson()

def ensure_outputs_exist():
    """
    Self-heal right before writes in case files were deleted while the app is running.
    Also repairs a CSV missing its header.
    """
    ensure_directories()

    # Ensure CSV has header
    try:
        with open(CSV_FILE, 'r', newline='') as f:
            first_line = f.readline()
            if not first_line or any(h not in first_line for h in ["Device ID", "UTC Time", "Local Date"]):
                _seed_csv()
    except FileNotFoundError:
        _seed_csv()

    # Ensure KML has closing </Document>
    try:
        with open(KML_FILE, 'r+', encoding='utf-8') as f:
            content = f.read()
            if "</Document>" not in content:
                _seed_kml()
    except FileNotFoundError:
        _seed_kml()

    # Ensure GeoJSON parses and has features list
    try:
        with open(GEOJSON_FILE, 'r+', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if not isinstance(data, dict) or data.get("type") != "FeatureCollection" or "features" not in data:
                    raise ValueError("Bad GeoJSON")
            except Exception:
                _seed_geojson()
    except FileNotFoundError:
        _seed_geojson()

# Seed at import time (also called before each write)
ensure_directories()

# =============================================================================
# Read-only Data Endpoints
# -----------------------------------------------------------------------------
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
    try:
        if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
            return "<i>No data yet</i>"
        df = pd.read_csv(CSV_FILE)
        if df.empty:
            return "<i>No data yet</i>"
        return df.to_html(classes="table table-striped", border=0)
    except Exception as e:
        return f"<b>Error loading CSV:</b> {e}"

# =============================================================================
# Envelope Decoding (Globalstar XML)
# -----------------------------------------------------------------------------
# Each <stuMessage> element contains:
#   <esn>  → Electronic Serial Number (device ID)
#   <uid>  → Optional Unique ID
#   <unixTime> → GPS-based epoch seconds (≈ UTC + 18s)
#   <gps> → Deprecated flag
#   <umn>, <gwa> → Optional
#   <payload> → Hex or ASCII payload (we use hex)
# =============================================================================
def _extract_globalstar_info(stu_root: ET.Element, msg_el: ET.Element) -> dict:
    esn = (msg_el.findtext("esn") or "").strip()
    uid = (msg_el.findtext("uid") or "").strip()
    gps_flag = (msg_el.findtext("gps") or "").strip()
    umn = (msg_el.findtext("umn") or "").strip()
    gwa = (msg_el.findtext("gwa") or "").strip()

    # unixTime in ICD is GPS time (about UTC+18s) -> convert to UTC string
    unix_gps = msg_el.findtext("unixTime") or ""
    utc_from_gps = ""
    try:
        gps_epoch = int(unix_gps)
        utc_dt = datetime.utcfromtimestamp(gps_epoch - 18)  # GPS -> UTC
        utc_from_gps = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    payload_el = msg_el.find("payload")
    p_len = payload_el.attrib.get("length", "") if payload_el is not None else ""
    p_src = payload_el.attrib.get("source", "") if payload_el is not None else ""
    p_enc = payload_el.attrib.get("encoding", "") if payload_el is not None else ""

    return {
        "bof_message_id": stu_root.attrib.get("messageID", ""),
        "bof_timestamp": stu_root.attrib.get("timeStamp", ""),
        "device_id": esn,            # Preferred device ID
        "modem_uid": uid,            # Fallback
        "unixTime_gps": unix_gps,
        "utc_from_unixTime": utc_from_gps,
        "gps_flag": gps_flag,
        "umn": umn,
        "gwa": gwa,
        "payload_length": p_len,
        "payload_source": p_src,
        "payload_encoding": p_enc,
    }

# =============================================================================
# Payload Decoding (fixed Kyberdyne layout)
# -----------------------------------------------------------------------------
# Message format (17 bytes total):
#   [0]    = burn byte 0x02 (ignore)
#   [1:5]  = latitude   (4B, big-endian) -> lat = raw/1e5 - 90
#   [5:9]  = longitude  (4B, big-endian) -> lon = raw/1e5 - 180
#   [9:13] = altitude   (4B, big-endian) -> alt_m = raw/100
#   [13:17]= time (UTC) (4B, big-endian) -> HHMMSSCC (we use HH:MM:SS)
# =============================================================================
def _hhmmss_from_cc(raw_u32: int) -> str:
    s = f"{raw_u32:08d}"[-8:]
    hh, mm, ss = s[0:2], s[2:4], s[4:6]
    return f"{hh}:{mm}:{ss}"

def _parse_fixed_payload(raw: bytes) -> dict:
    if len(raw) < 17:
        raise ValueError(f"payload too short: {len(raw)} bytes (need 17)")
    burn = raw[0]

    lat_u32 = int.from_bytes(raw[1:5],  "big", signed=False)
    lon_u32 = int.from_bytes(raw[5:9],  "big", signed=False)
    alt_u32 = int.from_bytes(raw[9:13], "big", signed=False)
    tim_u32 = int.from_bytes(raw[13:17],"big", signed=False)

    lat = round(lat_u32 / 1e5 - 90.0, 6)
    lon = round(lon_u32 / 1e5 - 180.0, 6)
    alt_m = int(alt_u32 / 100)
    alt_ft = round(alt_m * 3.28084, 2)
    utc_hms = _hhmmss_from_cc(tim_u32)

    # Derive local time crudely from longitude
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
        local_date = datetime.now().strftime("%d %b %y")
        local_time = utc_hms

    return {
        "device_id": "",  # envelope provides it
        "lat": lat,
        "lon": lon,
        "alt_m": alt_m,
        "alt_ft": alt_ft,
        "utc_time": utc_hms,
        "local_date": local_date,
        "local_time": local_time,
        "raw": f"bytes:{raw.hex()} (burn=0x{burn:02x}, fixed_layout_v1)"
    }

def _decode_from_hexstring(hex_text: str):
    cleaned = (hex_text or "").strip()
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]
    if len(cleaned) < 34:
        raise ValueError(f"hex payload too short: {len(cleaned)} hex chars (need 34)")
    raw = binascii.unhexlify(cleaned[:34])
    return _parse_fixed_payload(raw)

def decode_message(payload_b64: str):
    raw = base64.b64decode(payload_b64 or "")
    if len(raw) < 17:
        raise ValueError(f"base64 payload too short: {len(raw)} bytes (need 17)")
    return _parse_fixed_payload(raw[:17])

# =============================================================================
# Writers (CSV / KML / GeoJSON)
# -----------------------------------------------------------------------------
def append_csv(data):
    ensure_outputs_exist()
    with open(CSV_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            data.get('device_id', ''),
            data.get('utc_time', ''),
            data.get('local_date', ''),
            data.get('local_time', ''),
            data.get('lat', ''),
            data.get('lon', ''),
            data.get('alt_m', ''),
            data.get('alt_ft', ''),
            data.get('raw', '')
        ])

def append_kml(data):
    ensure_outputs_exist()
    placemark = (
        f"\n    <Placemark>\n"
        f"      <name>{data.get('device_id','')}</name>\n"
        f"      <description>Alt: {data.get('alt_m','')}m / {data.get('alt_ft','')}ft"
        f"\\nUTC: {data.get('utc_time','')}\\nRaw: {data.get('raw','')}</description>\n"
        f"      <Point><coordinates>{data.get('lon','')},{data.get('lat','')},{data.get('alt_m','')}</coordinates></Point>\n"
        f"    </Placemark>\n"
    )
    with open(KML_FILE, 'r+', encoding='utf-8') as f:
        content = f.read()
        if "</Document>" not in content:
            _seed_kml()
            content = open(KML_FILE, 'r', encoding='utf-8').read()
        updated = content.replace("</Document>", f"{placemark}</Document>")
        f.seek(0)
        f.write(updated)
        f.truncate()

def append_geojson(data):
    ensure_outputs_exist()
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [data['lon'], data['lat'], data['alt_m']]},
        "properties": {
            "device_id": data.get('device_id', ''),
            "alt_ft": data.get('alt_ft', ''),
            "utc_time": data.get('utc_time', ''),
            "local_date": data.get('local_date', ''),
            "local_time": data.get('local_time', ''),
            "raw": data.get('raw', '')
        }
    }
    with open(GEOJSON_FILE, 'r+', encoding='utf-8') as f:
        try:
            content = json.load(f)
        except Exception:
            content = {"type": "FeatureCollection", "features": []}
        content.setdefault('features', []).append(feature)
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
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

# =============================================================================
# Globalstar XML Ingest (POST /)
# -----------------------------------------------------------------------------
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
    if request.method == "GET":
        return "Kyberdyne Tracking Server Active"

    try:
        raw_body = request.data.decode("utf-8", errors="ignore")
        root_el = ET.fromstring(raw_body)
        corr_id = root_el.attrib.get("messageID", "unknown")

        processed = 0
        for child in root_el:
            # Handle ACKs (no payload decode)
            if child.tag.endswith("ackMessage"):
                # Could be logged/tracked here
                continue

            if child.tag.endswith("stuMessage"):
                payload_el = child.find("./payload")
                if payload_el is None or not (payload_el.text or "").strip():
                    continue

                encoding = (payload_el.attrib.get("encoding") or "").lower()
                payload_text = payload_el.text.strip()

                if encoding != "hex":
                    raise ValueError(f"Unsupported payload encoding: {encoding}")

                # Envelope info (ESN/UID/UnixTime GPS->UTC/etc.)
                info = _extract_globalstar_info(root_el, child)

                # Decode payload (strip optional '0x')
                payload_hex = payload_text[2:] if payload_text.lower().startswith("0x") else payload_text
                decoded = _decode_from_hexstring(payload_hex)

                # Merge & prefer ESN, fallback to UID
                record = {**decoded, **info}
                record["device_id"] = info.get("device_id") or info.get("modem_uid") or ""

                # Envelope audit in 'raw'
                record["raw"] = (
                    f"{decoded['raw']} | esn={info.get('device_id','')}"
                    f" uid={info.get('modem_uid','')}"
                    f" bofMsg={info.get('bof_message_id','')}"
                )

                _store_point(record)
                processed += 1

        msg = (f"{processed} messages received and stored successfully"
               if processed else "No stuMessage payloads found")
        return _xml_response(corr_id, state="pass", state_message=msg)

    except Exception as e:
        return _xml_response("error", state="fail", state_message=str(e))

# =============================================================================
# JSON Ingest (POST /message)
# -----------------------------------------------------------------------------
# Example body: {"payload": "BASE64_STRING"}
@app.route('/message', methods=['POST'])
def receive_message():
    try:
        payload = request.json.get('payload')
        if not payload:
            return jsonify({"error": "Missing payload"}), 400
        decoded = decode_message(payload)
        # No envelope on JSON route; device_id will be blank unless you extend schema
        _store_point(decoded)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# =============================================================================
# Dev Entry Point (Standalone Only)
# -----------------------------------------------------------------------------
# In production, run under Gunicorn:
#   ./venv/bin/gunicorn -w 1 -b 0.0.0.0:5050 process_messages:app
# =============================================================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050)
