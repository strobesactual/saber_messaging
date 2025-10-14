# app/api.py
from __future__ import annotations
from flask import Flask, jsonify, Response, send_file, request, current_app
import os
import csv
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from .decoding.payload_decoder import decode_from_hexstring, decode_b64
from .process_messages import process_incoming, set_tracker
from .storage import device_index
from . import record_messages


def _xml_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _stu_resp(state: str, detail: str = "", device_id: str = "") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    detail_xml = f"<detail>{_xml_escape(detail)}</detail>" if detail else ""
    did_xml = f"<deviceId>{_xml_escape(device_id)}</deviceId>" if device_id else ""
    return f"<stuResponseMsg><time>{ts}</time><state>{state}</state>{did_xml}{detail_xml}</stuResponseMsg>"

def _pick_first(d: dict, *names):
    for n in names:
        if n in d and (d[n] or "").strip():
            return d[n].strip()
    return ""

def register_routes(app: Flask, tracker, rec):
    # ---------- Globalstar XML ingest at ROOT ----------
    @app.post("/")
    def ingest_globalstar_xml():
        try:
            raw = request.data or b""
            if not raw:
                return Response(_stu_resp("fail", "empty body"), mimetype="text/xml")

            root = ET.fromstring(raw)

            def _strip(tag: str) -> str:
                if not isinstance(tag, str):
                    return ""
                return tag.split("}", 1)[-1]

            stu_nodes = [el for el in root if _strip(el.tag).lower() == "stumessage"]
            if not stu_nodes:
                # No stuMessage payloads (e.g., ack batches) — acknowledge and keep alive.
                return Response(_stu_resp("pass", "no stuMessage payload"), mimetype="text/xml")

            target = stu_nodes[0]

            # Flatten the target stuMessage (tags + attributes) into a lowercase map
            lower = {}
            for el in target.iter():
                tag_name = _strip(el.tag).lower()
                if not tag_name:
                    continue
                text = (el.text or "").strip()
                if tag_name not in lower or text:
                    lower[tag_name] = text
                for attr_name, attr_val in el.attrib.items():
                    attr_key = _strip(attr_name).lower()
                    lower[attr_key] = attr_val.strip()

            # Common tag names seen from Globalstar/VAR back offices
            device_id = _pick_first(lower, "mobileid", "deviceid", "esn", "originator", "uid")
            payload   = _pick_first(lower, "payload", "payloadhex", "data", "message")
            encoding  = lower.get("encoding", "").lower() or ("hex" if "payloadhex" in lower else "")

            if not device_id or not payload:
                current_app.logger.warning("ingest missing device_id/payload: tags=%s", list(lower.keys()))
                return Response(_stu_resp("fail", "missing device_id or payload"), mimetype="text/xml")

            # Hand off to our normal pipeline
            res = process_incoming({
                "device_id": device_id,
                "payload": payload,
                "encoding": encoding or None
            })

            if res.get("status") == "success":
                return Response(_stu_resp("pass", "ok", device_id), mimetype="text/xml")
            else:
                return Response(_stu_resp("fail", res.get("error", "unknown")), mimetype="text/xml")
        except Exception as e:
            return Response(_stu_resp("fail", str(e)), mimetype="text/xml")

    # ---------- Existing artifacts ----------
    @app.get("/data.csv")
    def get_csv():
        # Prefer new CSV, fall back to legacy if present
        csv_path = (
            getattr(rec, "CSV_LOG_PATH", None)
            or getattr(rec, "LOG_CSV_PATH", None)
            or getattr(rec, "CSV_FILE", None)
        )
        return send_file(csv_path, mimetype="text/csv", max_age=0)

    @app.get("/data.kml")
    def get_kml():
        return send_file(rec.KML_PATH, mimetype="application/vnd.google-earth.kml+xml", max_age=0)

    @app.get("/data.geojson")
    def get_geojson():
        return send_file(rec.GEOJSON_PATH, mimetype="application/geo+json", max_age=0)

    # ---------- Health ----------
    @app.get("/health")
    def health():
        return "OK", 200

    # ---------- Human live view (tolerant CSV read) ----------
    @app.get("/live")
    def live_view():
        try:
            csv_path = (
                getattr(rec, "CSV_LOG_PATH", None)
                or getattr(rec, "LOG_CSV_PATH", None)
                or getattr(rec, "CSV_FILE", None)
            )
            if not csv_path or not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
                return "<i>No data yet</i>"
            display_cols = [
                "Device ID", "UTC Time", "Local Date", "Local Time",
                "Latitude", "Longitude", "Altitude (m)", "Altitude (ft)",
                "Temp (K)", "Pressure (hPa)", "Raw Message"
            ]
            legacy_cols = [
                "ingest_time_utc", "device_id", "last_position_utc",
                "lat", "lon", "alt_m", "alt_ft", "temp_k", "pressure_hpa",
                "status", "questionable_data", "utc_time", "local_date", "local_time", "raw"
            ]
            latest_rows = []
            new_cols = getattr(rec, "CSV_FIELDS", []) or display_cols
            with open(csv_path, newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    if len(row) == len(new_cols):
                        if row == new_cols:
                            continue
                        mapped = dict(zip(new_cols, row))
                        latest_rows.append({
                            "Device ID": mapped.get("Device ID") or mapped.get("device_id", ""),
                            "UTC Time": mapped.get("UTC Time") or mapped.get("utc_time", ""),
                            "Local Date": mapped.get("Local Date") or mapped.get("local_date", ""),
                            "Local Time": mapped.get("Local Time") or mapped.get("local_time", ""),
                            "Latitude": mapped.get("Latitude") or mapped.get("lat", ""),
                            "Longitude": mapped.get("Longitude") or mapped.get("lon", ""),
                            "Altitude (m)": mapped.get("Altitude (m)") or mapped.get("alt_m", ""),
                            "Altitude (ft)": mapped.get("Altitude (ft)") or mapped.get("alt_ft", ""),
                            "Temp (K)": mapped.get("Temp (K)") or mapped.get("temp_k", ""),
                            "Pressure (hPa)": mapped.get("Pressure (hPa)") or mapped.get("pressure_hpa", ""),
                            "Raw Message": mapped.get("Raw Message") or mapped.get("raw", ""),
                        })
                    elif len(row) == len(display_cols):
                        if row == display_cols:
                            continue
                        mapped = dict(zip(display_cols, row))
                        latest_rows.append({col: mapped.get(col, "") for col in display_cols})
                    elif len(row) == len(legacy_cols):
                        if row == legacy_cols:
                            continue
                        mapped = dict(zip(legacy_cols, row))
                        latest_rows.append({
                            "Device ID": mapped.get("device_id", ""),
                            "UTC Time": mapped.get("utc_time", ""),
                            "Local Date": mapped.get("local_date", ""),
                            "Local Time": mapped.get("local_time", ""),
                            "Latitude": mapped.get("lat", ""),
                            "Longitude": mapped.get("lon", ""),
                            "Altitude (m)": mapped.get("alt_m", ""),
                            "Altitude (ft)": mapped.get("alt_ft", ""),
                            "Temp (K)": mapped.get("temp_k", ""),
                            "Pressure (hPa)": mapped.get("pressure_hpa", ""),
                            "Raw Message": mapped.get("raw", ""),
                        })
            if not latest_rows:
                return "<i>No data yet</i>"
            df = pd.DataFrame(latest_rows, columns=display_cols)
            return df.tail(200).to_html(classes="table table-striped", border=0)
        except Exception as e:
            return f"<b>Error loading CSV:</b> {e}"

    # ---------- JSON test harness still available ----------
    @app.post("/message")
    def receive_message():
        try:
            payload = request.json.get("payload")
            if not payload:
                return jsonify({"error": "Missing payload"}), 400
            decoded = decode_b64(payload)

            raw_val = decoded.get("raw", "")
            raw_val = raw_val.split(" ")[0]
            if raw_val.startswith("bytes:"):
                raw_val = raw_val[len("bytes:"):]
            decoded["raw"] = raw_val

            device_id = (request.json.get("device_id") or "").strip()
            if device_id:
                decoded["device_id"] = device_id

            _store_point(decoded, tracker, rec)
            return jsonify({"status": "success", "device_id": device_id})
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.get("/devices")
    def devices_all():
        try:
            return jsonify(tracker.get_all())
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/devices/<device_id>")
    def device_one(device_id):
        recd = tracker.get_one(device_id)
        if recd is None:
            return jsonify({"error": "not found"}), 404
        return jsonify(recd)

    @app.get("/devices_current")
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

def _store_point(data: dict, tracker, rec):
    # Keep the legacy helpers for JSON test route; XML route uses process_messages()
    from .record_messages import record_observation
    record_observation(data)
    try:
        tracker.update(data)
    except Exception as e:
        print(f"[tracker] update failed: {e}")
        
set_tracker(device_index)
app = Flask(__name__)
register_routes(app, device_index, record_messages)
