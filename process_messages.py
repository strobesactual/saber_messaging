# Kyberdyne Tracking Server

# Purpose:
#   - Receive Globalstar messages (XML/hex or JSON/Base64)
#   - Decode lat/lon/alt/timestamps robustly
#   - Persist points to CSV, KML, and GeoJSON
#   - Serve read-only endpoints for downstream tools and quick live views

# Runtime Notes (operational expectations):
#   - Designed for 24/7 ingestion behind a firewall/NAT with port-forward on TCP/5050.
#   - Globalstar BOF will POST XML with Content-Type=text/xml and expects an XML response.
#   - We reply with stuResponseMsg (xsi:noNamespaceSchemaLocation pointing to the official XSD).
#   - Max BOF wait per ICD is ~15s; keep handlers fast and avoid network I/O in the hot path.

# External (public) endpoints this app exposes (replace <HOST> with your FQDN or WAN IP):
#   - Health check     GET http://kyberdyne.ddns.net:5050/health
#   - Quick live view  GET http://kyberdyne.ddns.net:5050/live
#   - CSV artifact     GET http://kyberdyne.ddns.net:5050/data.csv
#   - KML artifact     GET http://kyberdyne.ddns.net:5050/data.kml
#   - GeoJSON artifact GET http://kyberdyne.ddns.net:5050/data.geojson
#
# Globalstar Back Office (BOF) interface key points you MUST honor:
#   - HTTP 1.1 POSTs arrive with Accept: text/xml and Content-Type: text/xml (NOT application/xml).
#   - You must return HTTP/200 with a well-formed stuResponseMsg/prvResponseMsg XML body.
#   - BOF may include both <stuMessage> and <ackMessage> in a batch; we ignore ackMessage.
#   - Known BOF egress IPs to allowlist at the gateway: 3.228.87.237, 34.231.245.76, 3.135.136.171, 3.133.245.206
#   - XSDs referenced in responses:
#       http://cody.glpconnect.com/XSD/StuResponse_Rev1_0.xsd
#
# Notes on output formats consumed downstream:
#   - CSV headers fixed; append-only.
#   - KML 2.2 (OGC) with one Placemark per point; coords order lon,lat,alt (meters).
#   - GeoJSON RFC 7946 FeatureCollection; geometry Point [lon, lat, alt_m]; properties carry meta.
#   - Coordinates preferred precision: DD.DDDDDD (6 decimal places).
#
# ------------------------------------------------------------------------------

from flask import Flask, request, jsonify, Response, send_file
import base64
import binascii
import os
import csv
import json
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from zoneinfo import ZoneInfo  # Python 3.9+

app = Flask(__name__)

try:
    from timezonefinder import TimezoneFinder
    _tzf = TimezoneFinder()
except Exception:
    _tzf = None

# === Paths ===
# Artifact locations on disk; these filenames are also the public “download” endpoints:
#   /data.csv    -> kyberdyne_tracking.csv  (append-only; human/audit friendly)
#   /data.kml    -> kyberdyne_tracking.kml  (Google Earth / GEarth Pro quick check)
#   /data.geojson-> kyberdyne_tracking.geojson (TAK/ATAK overlays, web map viewers)
#
# File growth expectations:
#   - CSV grows unbounded; rotate externally if needed.
#   - KML/GeoJSON append per point; we do simple in-place write—safe for low QPS.
TRACKING_DIR = "tracking_data"
CSV_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.csv")
KML_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.kml")
GEOJSON_FILE = os.path.join(TRACKING_DIR, "kyberdyne_tracking.geojson")

CSV_HEADER = [
    "Device ID", "UTC Time", "Local Date", "Local Time",
    "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
    "Temp (K)", "Pressure (hPa)", "Raw Message"
]


# =============================================================================
# Filesystem Setup / Self-Healing
# -----------------------------------------------------------------------------
# What this section guarantees:
#   - CSV has a valid header row.
#   - KML has a <Document> wrapper (OGC KML 2.2) even if the file is empty/corrupt.
#   - GeoJSON is always a valid {"type":"FeatureCollection","features":[...]} per RFC 7946.
#
# Why: Globalstar can hit us before an operator ever browses the artifacts; we must never 500
#      when /data.* endpoints are fetched by dashboards or monitoring.
# =============================================================================
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

# Seed at import time so /data.* never 404/500 on first boot
ensure_directories()

# =============================================================================
# Read-only Data Endpoints
# -----------------------------------------------------------------------------
# Contract:
#   - These routes must be cache-busted (max_age=0) so external tools always see latest.
#   - MIME types:
#       CSV: text/csv
#       KML: application/vnd.google-earth.kml+xml
#       GeoJSON: application/geo+json
# Quick checks:
#   curl -I http://<HOST>:5050/data.csv
#   curl -I http://<HOST>:5050/data.kml
#   curl -I http://<HOST>:5050/data.geojson
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
# Envelope helpers (namespace-agnostic)
# -----------------------------------------------------------------------------
# Why needed:
#   - BOF can send XML with namespaces; these helpers select by localname so we
#     don’t break if the xmlns prefix changes or is omitted.
# Caveats:
#   - We treat missing elements as empty strings and continue (ingestion should be robust).
# =============================================================================
def _first_by_localname(parent: ET.Element, name: str):
    lname = name.lower()
    for el in parent.iter():
        tag = el.tag
        if isinstance(tag, str) and tag.split('}')[-1].lower() == lname:
            return el
    return None

def _text_by_localname(parent: ET.Element, name: str, default: str = "") -> str:
    el = _first_by_localname(parent, name)
    return (el.text or "").strip() if el is not None and el.text is not None else default

# =============================================================================
# Payload Decoding (Kyberdyne fixed layout when length >= 17 bytes)
# -----------------------------------------------------------------------------
# Fixed layout mapping (big-endian; 25 bytes total):
#      [0]      burn/version (u8)
#      [1:5]    time_u32  (HHMMSS00) -> HH:MM:SS
#      [5:9]    lat_u32   -> (raw/1e5) - 90.0         -> DD.DDDDDD
#      [9:13]   lon_u32   -> (raw/1e5) - 180.0        -> DD.DDDDDD
#      [13:17]  alt_u32   -> meters = raw/100
#      [17:21]  temp_u32  -> Kelvin = raw/100
#      [21:25]  pres_u32  -> hPa    = raw/100
#
# Time handling:
#   - If the payload includes HHMMSS, we compute local time using a crude TZ from lon/15.
#   - If payload lacks time, we fall back to envelope unixTime (GPS epoch) minus 18s to UTC.
#
# Output precision:
#   - lat/lon: 6 decimals; alt_m int; alt_ft rounded(2).
# =============================================================================
def _hhmmss_from_cc(raw_u32: int) -> str:
    s = f"{raw_u32:08d}"[-8:]
    return f"{s[0:2]}:{s[2:4]}:{s[4:6]}"

def _parse_fixed_payload(raw: bytes) -> dict:
    if len(raw) < 25:
        raise ValueError(f"payload too short for fixed layout v2: {len(raw)} bytes")

    burn      = raw[0]
    time_u32  = int.from_bytes(raw[1:5],   "big", signed=False)
    lat_u32   = int.from_bytes(raw[5:9],   "big", signed=False)
    lon_u32   = int.from_bytes(raw[9:13],  "big", signed=False)
    alt_u32   = int.from_bytes(raw[13:17], "big", signed=False)
    temp_u32  = int.from_bytes(raw[17:21], "big", signed=False)
    pres_u32  = int.from_bytes(raw[21:25], "big", signed=False)

    lat    = round(lat_u32 / 1e5 - 90.0, 6)
    lon    = round(lon_u32 / 1e5 - 180.0, 6)
    alt_m  = round(alt_u32 / 100.0, 1)          # <-- keep the .1 precision (3.8 m)
    alt_ft = round(alt_m * 3.28084, 2)
    temp_k = round(temp_u32 / 100.0, 2)
    pressure_hpa = round(pres_u32 / 100.0, 2)

    utc_hms = _hhmmss_from_cc(time_u32)

    # Build a UTC datetime for "today" with that HH:MM:SS
    utc_today = datetime.now(timezone.utc).replace(microsecond=0)
    utc_dt = utc_today.replace(hour=int(utc_hms[0:2]),
                               minute=int(utc_hms[3:5]),
                               second=int(utc_hms[6:8]))

    # Prefer real TZ (DST-aware); fallback to lon/15 if unavailable
    local_dt = None
    if _tzf is not None:
        try:
            tzname = _tzf.timezone_at(lng=lon, lat=lat)
            if tzname:
                local_dt = utc_dt.astimezone(ZoneInfo(tzname))
        except Exception:
            local_dt = None

    if local_dt is None:
        try:
            offset_hours = round(lon / 15)
            # clamp
            if offset_hours < -12 or offset_hours > 14:
                offset_hours = 0
            local_dt = utc_dt.astimezone(timezone(timedelta(hours=offset_hours)))
        except Exception:
            local_dt = utc_dt

    local_date = local_dt.strftime("%d %b %y")
    local_time = local_dt.strftime("%H:%M:%S")

    return {
        "device_id": "",
        "lat": lat, "lon": lon,
        "alt_m": alt_m, "alt_ft": alt_ft,
        "temp_k": temp_k, "pressure_hpa": pressure_hpa,
        "utc_time": utc_hms,
        "local_date": local_date, "local_time": local_time,
        "raw": raw.hex()
    }


def _decode_from_hexstring(hex_text: str):
    cleaned = (hex_text or "").strip()
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]
    if len(cleaned) % 2 != 0:
        cleaned = "0" + cleaned
    try:
        raw = binascii.unhexlify(cleaned)
    except binascii.Error as e:
        raise ValueError(f"invalid hex payload: {e}")

    if len(raw) >= 25:
        return _parse_fixed_payload(raw[:25])

    # too short for v2 → store raw only
    return {
        "device_id": "",
        "lat": "", "lon": "",
        "alt_m": "", "alt_ft": "",
        "temp_k": "", "pressure_hpa": "",
        "utc_time": "", "local_date": "", "local_time": "",
        "raw": raw.hex()
    }


def decode_message(payload_b64: str):
    raw = base64.b64decode(payload_b64 or "")
    if len(raw) >= 25:
        return _parse_fixed_payload(raw[:25])
    return {
        "device_id": "",
        "lat": "", "lon": "",
        "alt_m": "", "alt_ft": "",
        "temp_k": "", "pressure_hpa": "",
        "utc_time": "", "local_date": "", "local_time": "",
        "raw": raw.hex()
    }


# =============================================================================
# Writers
# -----------------------------------------------------------------------------
# Persistence contract:
#   - append_csv: one row per point; columns fixed (see CSV_HEADER).
#   - append_kml: injects a Placemark before </Document>; safe idempotent pattern.
#   - append_geojson: appends Feature; preserves any prior features.
#
# Viewer guidance:
#   - CSV: spreadsheets, pandas, quick grep.
#   - KML: Google Earth (drag-drop the file), fast sanity check on path/altitude.
#   - GeoJSON: TAK/ATAK overlays, Mapbox/Leaflet, GIS tools.
# =============================================================================
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
            data.get('temp_k', ''),
            data.get('pressure_hpa', ''),
            data.get('raw', '')
        ])


def append_kml(data):
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
        f.seek(0)
        f.write(updated)
        f.truncate()


def append_geojson(data):
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
        f.seek(0)
        json.dump(content, f, indent=2)
        f.truncate()


def _store_point(data):
    append_csv(data)
    append_kml(data)
    append_geojson(data)

# =============================================================================
# Health
# -----------------------------------------------------------------------------
# Operational checks:
#   - /health → “OK” for LB/monitoring.
#   - /live → quick HTML table view of CSV (helpful to eyeball latest rows).
# =============================================================================
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

def _xml_response(correlation_id: str, state="pass", state_message="Store OK"):
    # Per ICD, the response is text/xml with stuResponseMsg and an ISO-ish timestamp (dd/MM/yyyy HH:mm:ss GMT)
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

# =============================================================================
# XML Ingest (POST /)
# -----------------------------------------------------------------------------
# Route for Globalstar BOF HTTP/HTTPS POSTs.
# Requirements pulled from ICD:
#   - Request:     POST /  Accept:text/xml  Content-Type:text/xml
#   - Response:    HTTP/200 + stuResponseMsg (text/xml)
#   - Behavior:    If no valid stuMessage elements → still return pass w/ “No stuMessage payloads found”
#   - IP control:  Ideally restrict at gateway to BOF IPs only (3.228.87.237, 34.231.245.76, 3.135.136.171, 3.133.245.206).
# Diagnostics:
#   - Logs print remote_addr, CT, CL, and a 200-char preview of the body.
# =============================================================================
@app.route("/", methods=["GET", "POST"])
def root():
    # GET: banner
    if request.method == "GET":
        return "Kyberdyne Tracking Server Active"

    try:
        # Request-level debug
        ct = request.headers.get("Content-Type", "")
        cl = request.headers.get("Content-Length", "")
        body_bytes = request.data or b""
        print(f"[RX] from={request.remote_addr} CT={ct} CL={cl} len(body)={len(body_bytes)}")

        raw_body = body_bytes.decode("utf-8", errors="ignore")
        preview = raw_body[:200].replace("\n", "\\n")
        print(f"[RX] body[:200]={preview}")

        root_el = ET.fromstring(raw_body)
        corr_id = root_el.attrib.get("messageID", "unknown")

        processed = 0
        for child in root_el.iter():
            tag_local = child.tag.split('}')[-1].lower() if isinstance(child.tag, str) else ""
            if tag_local == "ackmessage":
                continue
            if tag_local != "stumessage":
                continue

            payload_el = _first_by_localname(child, "payload")
            if payload_el is None or not (payload_el.text or "").strip():
                print("[RX] skip: no <payload> text")
                continue

            encoding = (payload_el.attrib.get("encoding") or "").lower().strip()
            payload_text = payload_el.text.strip()

            # Heuristic: accept if text looks like hex even when encoding isn't "hex"
            pt = payload_text[2:] if payload_text.lower().startswith("0x") else payload_text
            looks_hex = len(pt) >= 2 and all(c in "0123456789abcdefABCDEF" for c in pt[:min(len(pt), 64)])

            if encoding != "hex" and not looks_hex:
                print(f"[RX] unsupported encoding enc='{encoding}' sample='{payload_text[:40]}'")
                raise ValueError(f"Unsupported payload encoding: {encoding}")

            esn_dbg = _text_by_localname(child, "esn")
            uid_dbg = _text_by_localname(child, "uid")
            print(f"[RX] msg enc={encoding or '(none)'} payload_len={len(payload_text)} esn={esn_dbg} uid={uid_dbg}")

            # Decode payload
            payload_hex = pt
            decoded = _decode_from_hexstring(payload_hex)

            # Envelope fields
            esn = esn_dbg
            uid = uid_dbg
            unix_gps = _text_by_localname(child, "unixTime")  # GPS-based epoch
            bof_msg_id = root_el.attrib.get("messageID", "")

            # If payload lacked time, derive from unixTime (GPS -> UTC; GPS is ahead of UTC by ~18s)
            if not decoded.get("utc_time"):
                try:
                    gps_epoch = int(unix_gps)
                    utc_dt = datetime.utcfromtimestamp(gps_epoch - 18).replace(tzinfo=timezone.utc)
                    utc_time_str = utc_dt.strftime("%H:%M:%S")
                    # crude local tz from lon if numeric
                    lon = decoded.get("lon")
                    if isinstance(lon, (int, float)):
                        offset_hours = round(lon / 15)
                        if offset_hours < -12 or offset_hours > 14:
                            offset_hours = 0
                        local_dt = utc_dt.astimezone(timezone(timedelta(hours=offset_hours)))
                    else:
                        local_dt = utc_dt
                    decoded["utc_time"] = utc_time_str
                    decoded["local_date"] = local_dt.strftime("%d %b %y")
                    decoded["local_time"] = local_dt.strftime("%H:%M:%S")
                except Exception:
                    pass

            # Merge & store
            record = dict(decoded)
            record["device_id"] = esn or uid or decoded.get("device_id", "")

            # Keep raw as bare hex (strip older "bytes:" prefix if present)
            raw_val = decoded["raw"].split(" ")[0]
            if raw_val.startswith("bytes:"):
                raw_val = raw_val[len("bytes:"):]
            record["raw"] = raw_val

            _store_point(record)
            processed += 1

        msg = (f"{processed} messages received and stored successfully"
               if processed else "No stuMessage payloads found")
        return _xml_response(corr_id, state="pass", state_message=msg)

    except Exception as e:
        print(f"[ERR] {e.__class__.__name__}: {e}")
        return _xml_response("error", state="fail", state_message=str(e))

# =============================================================================
# JSON Ingest (POST /message)
# -----------------------------------------------------------------------------
# Purpose:
#   - Test harness and alternate non-BOF clients (e.g., replay tools, lambda/webhooks).
# Example:
#   curl -X POST http://<HOST>:5050/message \
#     -H 'Content-Type: application/json' \
#     -d '{"payload":"BASE64_STRING","device_id":"0-4737469"}'
# Guarantees:
#   - Writes to the same CSV/KML/GeoJSON pipelines as BOF XML.
# =============================================================================
@app.route('/message', methods=['POST'])
def receive_message():
    try:
        payload = request.json.get('payload')
        if not payload:
            return jsonify({"error": "Missing payload"}), 400

        decoded = decode_message(payload)

        # Ensure raw is bare hex even if decoders weren't updated yet
        raw_val = decoded.get("raw", "")
        raw_val = raw_val.split(" ")[0]
        if raw_val.startswith("bytes:"):
            raw_val = raw_val[len("bytes:"):]
        decoded["raw"] = raw_val

        device_id = (request.json.get('device_id') or "").strip()
        if device_id:
            decoded["device_id"] = device_id

        _store_point(decoded)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# =============================================================================
# Dev Entry Point (Standalone Only)
# -----------------------------------------------------------------------------
# Production standard:
#   - Run under Gunicorn, bound to 0.0.0.0:5050 (systemd service recommended).
#   - Example: ./venv/bin/gunicorn -w 1 -b 0.0.0.0:5050 process_messages:app
# Network requirements:
#   - Gateway must port-forward TCP/5050 from WAN to this host.
#   - Restrict source at the gateway to the four Globalstar BOF IPs when possible.
# Smoke tests:
#   - curl -I http://127.0.0.1:5050/health  -> 200
#   - curl -I http://127.0.0.1:5050/data.csv -> 200 (after first write, non-empty)
# =============================================================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050)
