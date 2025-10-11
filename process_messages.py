# Kyberdyne Tracking Server
# process_messages.py
#
# Purpose:
#   - Receive Globalstar messages (XML/hex or JSON/Base64)
#   - Decode lat/lon/alt/timestamps robustly
#   - Persist points to CSV, KML, and GeoJSON
#   - Serve read-only endpoints for downstream tools and quick live views
#
# Runtime Notes (operational expectations):
#   - Designed for 24/7 ingestion behind a firewall/NAT with port-forward on TCP/5050.
#   - Globalstar BOF will POST XML with Content-Type:text/xml and expects an XML response.
#   - We reply with stuResponseMsg (xsi:noNamespaceSchemaLocation pointing to the official XSD).
#   - Max BOF wait per ICD is ~15s; keep handlers fast and avoid network I/O in the hot path.
#
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
import os
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import pandas as pd

import device_index as tracker  # in-memory latest-state index (handles 'questionable_data')
from persist import (
    CSV_FILE, KML_FILE, GEOJSON_FILE,
    ensure_directories,
    append_csv, append_kml, append_geojson
)
from payloads import decode_from_hexstring, decode_b64  # v2 decoder (25-byte layout)
from cot_out import start_cot_publisher # Start CoT publisher (only if COT_URL is set in the environment)


start_cot_publisher(tracker.get_all)
app = Flask(__name__)

# ------------------------------------------------------------------------------
# Startup: seed artifacts and warm the in-memory index so /data.* never 404/500
# ------------------------------------------------------------------------------
ensure_directories()
try:
    tracker.warm_start(CSV_FILE)
except Exception as _e:
    print(f"[tracker] warm_start failed: {_e}")

# ------------------------------------------------------------------------------
# Read-only Data Endpoints (artifacts + quick HTML table)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# XML helpers (namespace-agnostic selects)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# Store pipeline (CSV/KML/GeoJSON + device_index)
# ------------------------------------------------------------------------------
def _store_point(data: dict):
    append_csv(data)
    append_kml(data)
    append_geojson(data)
    try:
        # tracker.update() will mark 'questionable_data' and 'carried_fields' if it
        # has to preserve last good values for any blank fields in this message.
        tracker.update(data)
    except Exception as e:
        print(f"[tracker] update failed: {e}")

# ------------------------------------------------------------------------------
# Health + BOF XML response builder
# ------------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

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

# ------------------------------------------------------------------------------
# XML Ingest (POST /) — Globalstar BOF pushes land here
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET", "POST"])
def root():
    # Banner for simple GETs
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

            # Accept if text looks like hex even when encoding isn't "hex"
            pt = payload_text[2:] if payload_text.lower().startswith("0x") else payload_text
            looks_hex = len(pt) >= 2 and all(c in "0123456789abcdefABCDEF" for c in pt[:min(len(pt), 64)])
            if encoding != "hex" and not looks_hex:
                print(f"[RX] unsupported encoding enc='{encoding}' sample='{payload_text[:40]}'")
                raise ValueError(f"Unsupported payload encoding: {encoding}")

            esn_dbg = _text_by_localname(child, "esn")
            uid_dbg = _text_by_localname(child, "uid")
            print(f"[RX] msg enc={encoding or '(none)'} payload_len={len(payload_text)} esn={esn_dbg} uid={uid_dbg}")

            # Decode payload (new fixed layout v2)
            decoded = decode_from_hexstring(pt)

            # Envelope fields
            esn = esn_dbg
            uid = uid_dbg
            unix_gps = _text_by_localname(child, "unixTime")  # GPS-based epoch

            # If payload lacked time, derive from unixTime (GPS -> UTC; GPS is ahead of UTC by ~18s)
            if not decoded.get("utc_time"):
                try:
                    gps_epoch = int(unix_gps)
                    utc_dt = datetime.utcfromtimestamp(gps_epoch - 18).replace(tzinfo=timezone.utc)
                    utc_time_str = utc_dt.strftime("%H:%M:%S")
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

            raw_val = decoded.get("raw", "")
            raw_val = raw_val.split(" ")[0]
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

# ------------------------------------------------------------------------------
# JSON Ingest (POST /message) — test harness / alternate clients
# ------------------------------------------------------------------------------
@app.route('/message', methods=['POST'])
def receive_message():
    try:
        payload = request.json.get('payload')
        if not payload:
            return jsonify({"error": "Missing payload"}), 400

        decoded = decode_b64(payload)

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

# ------------------------------------------------------------------------------
# Device Index Views (JSON + simple HTML)
# ------------------------------------------------------------------------------
@app.route("/devices", methods=["GET"])
def devices_all():
    try:
        return jsonify(tracker.get_all())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/devices/<device_id>", methods=["GET"])
def device_one(device_id):
    rec = tracker.get_one(device_id)
    if rec is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(rec)

@app.route("/devices.html", methods=["GET"])
def devices_html():
    try:
        rows = tracker.get_all()
        html = ["<table border=0 class='table table-striped'>",
                "<tr><th>Device ID</th><th>Status</th><th>Q?</th><th>Last Seen (UTC)</th>"
                "<th>Lat</th><th>Lon</th><th>Alt (m)</th><th>Temp (K)</th><th>Pressure (hPa)</th></tr>"]
        for r in rows:
            q = "&#10003;" if r.get('questionable_data', False) else ""
            html.append(
                f"<tr><td>{r.get('device_id','')}</td>"
                f"<td>{r.get('status','')}</td>"
                f"<td title='carried: {r.get('carried_fields', [])}'>{q}</td>"
                f"<td>{r.get('last_seen_utc','')}</td>"
                f"<td>{r.get('lat','')}</td>"
                f"<td>{r.get('lon','')}</td>"
                f"<td>{r.get('alt_m','')}</td>"
                f"<td>{r.get('temp_k','')}</td>"
                f"<td>{r.get('pressure_hpa','')}</td></tr>"
            )
        html.append("</table>")
        return "\n".join(html)
    except Exception as e:
        return f"<b>tracker error:</b> {e}", 500

@app.route("/devices_current", methods=["GET"])
def devices_current():
    rows = tracker.get_all()
    out = []
    for r in rows:
        out.append({
            "device_id": r.get("device_id",""),
            "status": r.get("status",""),
            "questionable_data": r.get("questionable_data", False),
            "carried_fields": r.get("carried_fields", []),
            "last_seen_utc": r.get("last_seen_utc",""),
            "last_position_utc": r.get("last_position_utc",""),
            "position_age_min": r.get("position_age_min",""),
            "lat": r.get("lat",""),
            "lon": r.get("lon",""),
            "alt_m": r.get("alt_m",""),
        })
    return jsonify(out)

# ------------------------------------------------------------------------------
# Dev Entry Point (Standalone Only)
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050)

# ---------- End of File ----------
