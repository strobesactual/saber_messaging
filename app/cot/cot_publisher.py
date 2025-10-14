# app/cot/cot_publisher.py
# CoT/TAK publisher thread using asyncio + raw XML events (no CoT lib dependency)

import os, ssl, asyncio, threading, logging, sqlite3
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape
from pathlib import Path

log = logging.getLogger("cot")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# Resolve DB path from config if available
try:
    from ..config import DB_PATH as _CFG_DB_PATH
    DB_PATH = Path(_CFG_DB_PATH)
except Exception:
    DB_PATH = Path("tracking_data/device_latest.db")

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

    log.info(
        "[cot] TLS config: ca=%s (ok=%s sz=%s) cert=%s (ok=%s sz=%s) key=%s (ok=%s sz=%s) check_hostname=%s",
        ca or "<unset>", ca_ok, ca_sz,
        cert or "<unset>", cert_ok, cert_sz,
        key or "<unset>", key_ok, key_sz,
        not no_host_check,
    )

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

def _iso(dt: datetime) -> str:
    # 2025-10-14T03:07:11Z
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _status_color(status: str) -> str:
    s = (status or "").upper()
    if s == "IN_FLIGHT":
        return "Green"
    if s == "ON_GROUND":
        return "Blue"
    if s == "STALE":
        return "Red"
    return "Yellow"

def _float_or_default(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)

def _build_cot_xml(*, device_id: str, lat: float, lon: float, alt_m: float,
                   utc_time: str, local_date: str, local_time: str, status: str) -> str:
    now = datetime.now(timezone.utc)
    time_s = _iso(now)
    start_s = time_s
    stale_s = _iso(now + timedelta(minutes=2))

    # sanitize
    did = escape(device_id or "")
    remarks = escape(
        f"UTC: {utc_time}\nLocal: {local_date} {local_time}\nAltitude: {alt_m}m\nStatus: {status}"
    )
    color = escape(_status_color(status))

    # ce/le large to indicate unknown accuracy (common practice)
    # hae = altitude above ellipsoid (we'll send meters we have)
    xml = (
        f'<event version="2.0" type="a-f-G-U-C" uid="{did}" '
        f'time="{time_s}" start="{start_s}" stale="{stale_s}" how="h-g-i-g-o">'
        f'<point lat="{lat:.6f}" lon="{lon:.6f}" hae="{alt_m:.1f}" ce="9999999" le="9999999"/>'
        f'<detail>'
        f'<contact callsign="{did}"/>'
        f'<remarks>{remarks}</remarks>'
        f'<color>{color}</color>'
        f'</detail>'
        f'</event>'
    )
    return xml

async def _publish_cot(url: str):
    u = urlparse(url)
    if u.scheme not in ("ssl", "tls"):
        raise ValueError(f"Unsupported COT_URL scheme {u.scheme!r}; use ssl://host:port")

    host, port = (u.hostname or ""), (u.port or 0)
    if not host or not port:
        raise ValueError(f"COT_URL missing host/port: {url!r}")

    ssl_ctx = _build_ssl_context()
    server_hostname = host if ssl_ctx.check_hostname else None

    log.info("[cot] connecting to %s:%s (hostname check=%s)", host, port, ssl_ctx.check_hostname)
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx, server_hostname=server_hostname)
    log.info("[cot] connected to %s:%s", host, port)

    query = (
        "SELECT device_id, utc_time, local_date, local_time, "
        "       lat, lon, alt_m, status "
        "FROM device_latest"
    )

    while True:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()

            for (device_id, utc_time, local_date, local_time, lat, lon, alt_m, status) in rows:
                if not device_id:
                    continue

                # Harden fields
                lat = _float_or_default(lat, 0.0)
                lon = _float_or_default(lon, 0.0)
                alt_m = _float_or_default(alt_m, 0.0)

                # Clamp to valid ranges
                if not (-90.0 <= lat <= 90.0):
                    lat = 0.0
                if not (-180.0 <= lon <= 180.0):
                    lon = 0.0
                if alt_m < 0:
                    alt_m = 0.0

                xml = _build_cot_xml(
                    device_id=device_id, lat=lat, lon=lon, alt_m=alt_m,
                    utc_time=utc_time or "", local_date=local_date or "", local_time=local_time or "",
                    status=status or "UNKNOWN",
                )

                writer.write((xml + "\n").encode("utf-8"))
                await writer.drain()
                log.info("[cot] published event uid=%s status=%s lat=%.6f lon=%.6f alt=%.1f",
                         device_id, status, lat, lon, alt_m)

        except Exception as e:
            log.exception("[cot] publish loop error: %s", e)

        # send updates every minute
        await asyncio.sleep(60)

async def _connect_and_publish():
    url = os.getenv("COT_URL", "").strip()
    if not url:
        log.info("[cot] COT_URL not set; CoT publisher not started.")
        return

    while True:
        try:
            await _publish_cot(url)
        except Exception as e:
            log.exception("[cot] publish failed: %s: %s", type(e).__name__, e)
            await asyncio.sleep(5)

def _runner():
    log.info("[cot] publisher thread started.")
    asyncio.run(_connect_and_publish())

def start_cot_publisher():
    t = threading.Thread(target=_runner, name="cot-publisher", daemon=True)
    t.start()