# app/api.py

from __future__ import annotations
from flask import Flask, jsonify, Response, send_file, request
import os
import pandas as pd
from .decoding.payload_decoder import decode_from_hexstring, decode_b64

def register_routes(app: Flask, tracker, rec):
    # Artifacts
    @app.get("/data.csv")
    def get_csv():
        return send_file(rec.CSV_FILE, mimetype="text/csv", max_age=0)

    @app.get("/data.kml")
    def get_kml():
        return send_file(rec.KML_FILE, mimetype="application/vnd.google-earth.kml+xml", max_age=0)

    @app.get("/data.geojson")
    def get_geojson():
        return send_file(rec.GEOJSON_FILE, mimetype="application/geo+json", max_age=0)

    # Simple health/live
    @app.get("/health")
    def health():
        return "OK", 200

    @app.get("/live")
    def live_view():
        try:
            if not os.path.exists(rec.CSV_FILE) or os.path.getsize(rec.CSV_FILE) == 0:
                return "<i>No data yet</i>"
            df = pd.read_csv(rec.CSV_FILE)
            if df.empty:
                return "<i>No data yet</i>"
            return df.to_html(classes="table table-striped", border=0)
        except Exception as e:
            return f"<b>Error loading CSV:</b> {e}"

    # JSON ingest (test harness)
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

            # persist + index
            _store_point(decoded, tracker, rec)
            return jsonify({"status": "success"})
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    # Device index views
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
    rec.append_csv(data)
    rec.append_kml(data)
    rec.append_geojson(data)
    try:
        tracker.update(data)
        # keep device_latest.csv in sync
        rec.write_latest_row(data)
    except Exception as e:
        print(f"[tracker] update failed: {e}")
