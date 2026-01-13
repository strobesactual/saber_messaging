#!/usr/bin/env python3
"""
One-time CoT fleet sender.

Reads all rows from tracking_data/device_latest.db, builds CoT events for each
device with valid lat/lon, and sends them over TLS to the configured TAK
endpoint. Uses the same client cert/key/CA bundle you configured for the
running service (edit paths below or set env vars).
"""

from __future__ import annotations

import os
import ssl
import socket
import sqlite3
import time
from urllib.parse import urlparse

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "tracking_data", "device_latest.db")

# Defaults pulled from current setup; override via env if needed.
COT_URL = os.getenv("COT_URL", "ssl://192.168.30.62:8089")
CLIENT_CERT = os.getenv("PYTAK_TLS_CLIENT_CERT", "/home/austin/tak-certs/saber_server.pem")
CLIENT_KEY = os.getenv("PYTAK_TLS_CLIENT_KEY", "/home/austin/tak-certs/saber_server.key")
CA_FILE = os.getenv("PYTAK_TLS_CA_CERT", "/home/austin/tak-certs/ca.pem")
MARKER_TYPE = os.getenv("COT_MARKER_TYPE", "a-f-A").strip() or "a-f-A"


def _build_cot(device_id: str, lat: float, lon: float, alt_m: float, ts_iso: str, callsign: str) -> str:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    stale = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 30 * 24 * 60 * 60))
    t = ts_iso or now
    return (
        f'<event version="2.0" uid="{device_id}-fleet" type="{MARKER_TYPE}" '
        f'time="{t}" start="{t}" stale="{stale}" how="m-g">'
        f'<point lat="{lat:.6f}" lon="{lon:.6f}" hae="{alt_m:.1f}" ce="0" le="0"/>'
        f'<detail><contact callsign="{callsign}"/><remarks>Fleet push</remarks></detail>'
        f'</event>'
    )


def _valid_latlon(lat: float, lon: float) -> bool:
    try:
        return -90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0
    except Exception:
        return False


def main() -> int:
    u = urlparse(COT_URL)
    if u.scheme not in ("ssl", "tls"):
        raise SystemExit(f"Unsupported COT_URL scheme: {u.scheme}")
    host, port = u.hostname, u.port
    if not host or not port:
        raise SystemExit(f"COT_URL missing host/port: {COT_URL}")

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CA_FILE)
    ctx.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
    # Use CN/SNI of takserver when connecting by IP to satisfy cert matching
    server_name = u.hostname or "takserver"
    if server_name.replace(".", "").isdigit():
        server_name = "takserver"

    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT device_id, lat, lon, alt_m, last_position_utc FROM device_latest ORDER BY last_position_utc DESC"
    ).fetchall()
    con.close()

    payloads = []
    for did, lat, lon, alt_m, ts_iso in rows:
        if not did:
            continue
        if not _valid_latlon(lat, lon):
            continue
        try:
            alt = float(alt_m or 0.0)
        except Exception:
            alt = 0.0
        cs = f"SR{str(did)[-2:]}" if str(did)[-2:].isdigit() else "SR00"
        payloads.append(_build_cot(did, float(lat), float(lon), alt, ts_iso or "", cs))

    if not payloads:
        print("No valid rows to send.")
        return 0

    with socket.create_connection((host, port), timeout=10) as sock:
        with ctx.wrap_socket(sock, server_hostname=server_name) as ssock:
            for xml in payloads:
                ssock.sendall(xml.encode("utf-8") + b"\n")
            print(f"Sent {len(payloads)} CoT events to {host}:{port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
