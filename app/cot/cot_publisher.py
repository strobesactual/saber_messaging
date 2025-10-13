# app/cot_publisher.py


import os, ssl, asyncio, threading, logging, csv
from urllib.parse import urlparse
from datetime import datetime
from pytak import CoT

log = logging.getLogger("cot")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

def _boolish(v: str | None) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() not in ("", "0", "false", "no", "off")

def _build_ssl_context():
    ca = os.getenv("PYTAK_TLS_CA_CERT", "").strip()
    cert = os.getenv("PYTAK_TLS_CLIENT_CERT", "").strip()
    key = os.getenv("PYTAK_TLS_CLIENT_KEY", "").strip()
    no_host_check = _boolish(os.getenv("PYTAK_TLS_DONT_CHECK_HOSTNAME", ""))

    def _exist(p): 
        try: 
            return os.path.isfile(p), (os.path.getsize(p) if os.path.isfile(p) else 0)
        except Exception:
            return False, 0

    ca_ok, ca_sz = _exist(ca)
    cert_ok, cert_sz = _exist(cert)
    key_ok, key_sz = _exist(key)

    log.info("[cot] TLS config: ca=%s (ok=%s sz=%s) cert=%s (ok=%s sz=%s) key=%s (ok=%s sz=%s) check_hostname=%s",
             ca or "<unset>", ca_ok, ca_sz,
             cert or "<unset>", cert_ok, cert_sz,
             key or "<unset>", key_ok, key_sz,
             not no_host_check)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if ca_ok:
        ctx.load_verify_locations(cafile=ca)
    else:
        ctx.load_default_certs()

    if cert_ok and key_ok:
        ctx.load_cert_chain(certfile=cert, keyfile=key)

    ctx.check_hostname = not no_host_check
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

async def _publish_cot(url: str, csv_path: str):
    u = urlparse(url)
    if u.scheme not in ("ssl", "tls"):
        raise ValueError(f"Unsupported COT_URL scheme {u.scheme!r}; use ssl://host:port")

    host, port = (u.hostname or ""), (u.port or 0)
    if not host or not port:
        raise ValueError(f"COT_URL missing host/port: {url!r}")

    ssl_ctx = _build_ssl_context()
    server_hostname = host if ssl_ctx.check_hostname else None

    log.info("[cot] connecting to %s:%s (hostname check=%s)", host, port)
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx, server_hostname=server_hostname)
    log.info("[cot] connected to %s:%s", host, port)

    while True:
        with open(csv_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                device_id = row.get("Device ID", "")
                if not device_id:
                    continue

                lat = float(row.get("Latitude", 0.0))
                lon = float(row.get("Longitude", 0.0))
                alt = float(row.get("Altitude (m)", 0.0))
                status = row.get("Status", "UNKNOWN").upper()

                # Color coding based on status
                color = "Yellow"  # Default
                if status == "IN_FLIGHT":
                    color = "Green"
                elif status == "ON_GROUND":
                    color = "Blue"
                elif status == "STALE":
                    color = "Red"

                cot = CoT(
                    event_type="a-f-G-U-C",
                    uid=device_id,
                    point=CoT.Point(lat=lat, lon=lon, hae=alt),
                    detail=CoT.Detail(
                        contact=CoT.Contact(callsign=device_id),
                        remarks=f"Altitude: {alt}m\nStatus: {status}",
                        color=color
                    )
                )
                writer.write(cot.xml().encode())
                await writer.drain()
                log.info(f"[cot] Published CoT for {device_id} with status {status}")

        await asyncio.sleep(60)  # Refresh every minute

async def _connect_and_publish():
    url = os.getenv("COT_URL", "").strip()
    csv_path = os.path.join("tracking_data", "device_latest.csv")
    if not url:
        log.info("[cot] COT_URL not set; CoT publisher not started.")
        return

    while True:
        try:
            await _publish_cot(url, csv_path)
        except Exception as e:
            log.exception("[cot] publish failed: %s: %s", type(e).__name__, e)
            await asyncio.sleep(5)

def _runner():
    log.info("[cot] publisher thread started.")
    asyncio.run(_connect_and_publish())

def start_cot_publisher():
    t = threading.Thread(target=_runner, name="cot-publisher", daemon=True)
    t.start()