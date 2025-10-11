# views.py
from flask import Blueprint, jsonify
import device_index as tracker

views_bp = Blueprint("views", __name__)

@views_bp.route("/devices", methods=["GET"])
def devices_all():
    try:
        return jsonify(tracker.get_all())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@views_bp.route("/devices/<device_id>", methods=["GET"])
def device_one(device_id):
    rec = tracker.get_one(device_id)
    if rec is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(rec)

@views_bp.route("/devices.html", methods=["GET"])
def devices_html():
    try:
        rows = tracker.get_all()
        html = ["<table border=0 class='table table-striped'>",
                "<tr><th>Device ID</th><th>Status</th><th>Q?</th>"
                "<th>Last Seen (UTC)</th><th>Lat</th><th>Lon</th>"
                "<th>Alt (m)</th><th>Temp (K)</th><th>Pressure (hPa)</th></tr>"]
        for r in rows:
            q = "&#10003;" if r.get('questionable_data', False) else ""
            html.append(
                f"<tr><td>{r.get('device_id','')}</td>"
                f"<td>{r.get('status','')}</td>"
                f"<td>{q}</td>"
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

@views_bp.route("/devices_current", methods=["GET"])
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


