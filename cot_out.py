# cot_out.py
#
# Purpose:
#   - Convert latest device records (from device_index) into CoT <event/>s
#   - Push CoT to a TAK Server using PyTAK transport (UDP or TLS)
#
# Runtime Notes:
#   - Starts a background thread (daemon) inside the Flask/Gunicorn process.
#   - Requires environment variable COT_URL to be set, e.g.:
#       UDP test:  COT_URL=udp://192.168.1.62:8087
#       TLS prod:  COT_URL=ssl://192.168.1.62:8089
#     TLS envs (if using ssl://):
#       TAKSERVER_TLS_CLIENT_CERT=/home/austin/tak-certs/saber-feed.pem
#       TAKSERVER_TLS_CLIENT_KEY=/home/austin/tak-certs/saber-feed.key
#       TAKSERVER_TLS_ROOT_CERT=/home/austin/tak-certs/ca.pem
#
# CoT event details:
#   - type: defaults to 'a-f-A' (friendly / air). Override via COT_TYPE env.
#   - time/start: now (UTC). stale: now + COT_STALE_SECS (default 120 s).
#   - point: lat/lon from tracker; hae=alt_m; ce/le set large when unknown.
#   - contact.callsign: device_id. remarks: temp/pressure/flags.
#
# References:
#   - PyTAK config/envs and ports (8087 UDP, 8089 TLS): see PyPI docs.
#   - CoT basics: event/point/time/start/stale, contact, remarks.

from __future__ import annotations
import asyncio
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Any

import pytak  # transport to TAK Server (UDP/TLS)

# Tunables via env
COT_URL = os.getenv("COT_URL")                       # e.g., udp://192.168.1.62:8087 or ssl://192.168.1.62:8089
COT_TYPE = os.getenv("COT_TYPE", "a-f-A")            # friendly / air (generic)
COT_PUSH_INTERVAL = int(os.getenv("COT_PUSH_INTERVAL", "5"))  # seconds
COT_STALE_SECS = int(os.getenv("COT_STALE_SECS", "120"))

def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _iso_future(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

def _fmt(val, nd=6):
    try:
        return f"{float(val):.{nd}f}"
    except Exception:
        return ""

def _build_cot(d: Dict[str, Any]) -> bytes | None:
    """Return a minimal, valid CoT event for a device row, or None if no position."""
    lat = d.get("lat")
    lon = d.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None  # no fix yet

    alt_m = d.get("alt_m")
    try:
        hae = float(alt_m) if alt_m not in ("", None) else 0.0
    except Exception:
        hae = 0.0

    uid = f"balloon:{d.get('device_id','unknown')}"
    callsign = d.get("device_id", "unknown")
    now = _iso_now()
    stale = _iso_future(COT_STALE_SECS)

    # Build a compact CoT event (version 2.0)
    # how="m-g" = machine-generated; ce/le big when unknown
    remarks_bits = []
    if d.get("status"): remarks_bits.append(f"status={d['status']}")
    if d.get("questionable_data"): remarks_bits.append("questionable=true")
    if d.get("temp_k") not in ("", None): remarks_bits.append(f"T={d['temp_k']}K")
    if d.get("pressure_hpa") not in ("", None): remarks_bits.append(f"P={d['pressure_hpa']}hPa")
    remarks = " | ".join(remarks_bits) if remarks_bits else "saber_tracking"

    xml = (
        f'<event version="2.0" uid="{uid}" type="{COT_TYPE}" '
        f'time="{now}" start="{now}" stale="{stale}" how="m-g">'
        f'<point lat="{_fmt(lat,6)}" lon="{_fmt(lon,6)}" hae="{_fmt(hae,1)}" ce="9999999.0" le="9999999.0"/>'
        f'<detail><contact callsign="{callsign}"/><remarks>{remarks}</remarks></detail>'
        f'</event>'
    )
    return xml.encode("utf-8")

class SaberProducer(pytak.QueueWorker):
    """Producer that reads the in-memory device list and enqueues CoT when changed or on interval."""
    def __init__(self, queue, clitool: pytak.CLITool, get_devices: Callable[[], list[Dict[str, Any]]]):
        super().__init__(queue, clitool)
        self._get_devices = get_devices
        self._last_sig: Dict[str, tuple] = {}

    async def run(self, *_args, **_kwargs):
        while True:
            try:
                devices = self._get_devices()
                for d in devices:
                    dev_id = d.get("device_id")
                    cot = _build_cot(d)
                    if not cot or not dev_id:
                        continue
                    sig = (d.get("lat"), d.get("lon"), d.get("alt_m"))
                    if self._last_sig.get(dev_id) != sig:
                        await self.put_queue(cot)
                        self._last_sig[dev_id] = sig
            except Exception as e:
                print(f"[cot] producer error: {e}")
            await asyncio.sleep(COT_PUSH_INTERVAL)

async def _amain(get_devices):
    if not COT_URL:
        print("[cot] COT_URL not set; CoT publisher disabled.")
        return
    # PyTAK reads COT_URL and any TAKSERVER_TLS_* envs automatically.
    clitool = pytak.CLITool()
    tx_queue = asyncio.Queue()
    client = await pytak.ClientFactory(clitool).build(tx_queue)
    producer = SaberProducer(tx_queue, clitool, get_devices)
    print(f"[cot] starting → {COT_URL}")
    await asyncio.gather(producer.run(), client.run())

def start_cot_publisher(get_devices):
    """Start the CoT publisher in a daemon thread (kept simple since Gunicorn uses 1 worker)."""
    if not COT_URL:
        print("[cot] COT_URL not set; skipping CoT publisher.")
        return
    def _runner():
        asyncio.run(_amain(get_devices))
    t = threading.Thread(target=_runner, name="cot-publisher", daemon=True)
    t.start()
    print("[cot] publisher thread started.")

