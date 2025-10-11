from __future__ import annotations
import asyncio, os, threading
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Any, List

import pytak
import device_index as tracker

COT_URL = os.getenv("COT_URL")
COT_TYPE = os.getenv("COT_TYPE", "a-f-A")
COT_PUSH_INTERVAL = int(os.getenv("COT_PUSH_INTERVAL", "5"))
COT_STALE_SECS = int(os.getenv("COT_STALE_SECS", "300"))

def _pytak_config() -> dict:
    cfg = {}
    if COT_URL: cfg["COT_URL"] = COT_URL
    # PyTAK 6.x looks for these keys
    for k in ("TAKSERVER_TLS_CLIENT_CERT","TAKSERVER_TLS_CLIENT_KEY","TAKSERVER_TLS_ROOT_CERT",
              "PYTAK_TLS_CLIENT_CERT","PYTAK_TLS_CLIENT_KEY","PYTAK_TLS_CA_CERT"):
        v = os.getenv(k)
        if v: cfg[k] = v
    return cfg

def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _iso_future(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

def _fmt(val, nd=6):
    try: return f"{float(val):.{nd}f}"
    except Exception: return ""

def _build_cot(d: Dict[str, Any]) -> bytes | None:
    lat, lon = d.get("lat"), d.get("lon")
    if not isinstance(lat,(int,float)) or not isinstance(lon,(int,float)): return None
    try: hae = float(d.get("alt_m") or 0.0)
    except Exception: hae = 0.0
    uid = f"balloon:{d.get('device_id','unknown')}"
    callsign = d.get("device_id","unknown")
    now = _iso_now(); stale = _iso_future(COT_STALE_SECS)
    remarks_bits=[]
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
    def __init__(self, queue, clitool: pytak.CLITool, get_devices: Callable[[], List[Dict[str, Any]]]):
        super().__init__(queue, clitool)
        self._get_devices = get_devices
        self._last_sig: Dict[str, tuple] = {}
    async def run(self, *_args, **_kwargs):
        while True:
            try:
                for d in self._get_devices():
                    dev_id = d.get("device_id")
                    if not dev_id: continue
                    sig = (d.get("lat"), d.get("lon"), d.get("alt_m"))
                    if self._last_sig.get(dev_id) == sig: continue
                    cot = _build_cot(d)
                    if cot:
                        await self.put_queue(cot)
                        self._last_sig[dev_id] = sig
            except Exception as e:
                print(f"[cot] producer error: {e}")
            await asyncio.sleep(COT_PUSH_INTERVAL)

async def _amain(get_devices):
    if not COT_URL:
        print("[cot] COT_URL not set; CoT publisher disabled."); return
    config = _pytak_config()
    clitool = pytak.CLITool(config)
    await clitool.setup()  # IMPORTANT for PyTAK 6.x
    tx_queue = asyncio.Queue()
    client = await pytak.ClientFactory(clitool).build(tx_queue)
    producer = SaberProducer(tx_queue, clitool, get_devices)
    print(f"[cot] starting → {config.get('COT_URL', COT_URL)}")
    await asyncio.gather(producer.run(), client.run())

def start_cot_publisher(get_devices):
    if not COT_URL:
        print("[cot] COT_URL not set; skipping CoT publisher."); return
    # default to latest-per-device from tracker
    if get_devices is None:
        try:
            from process_messages import CSV_FILE, ensure_directories
            ensure_directories(); tracker.warm_start(CSV_FILE)
        except Exception as e:
            print(f"[cot] warm_start failed: {e}")
        get_devices = tracker.get_all

    def _runner():
        asyncio.run(_amain(get_devices))
    t = threading.Thread(target=_runner, name="cot-publisher", daemon=True)
    t.start()
    print("[cot] publisher thread started (PyTAK 6.x).")
