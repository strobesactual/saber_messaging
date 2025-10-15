# app/cot/cot_publisher.py
# ---------------------------------------------------------------------------
# Responsibility:
#   - Periodically read latest device rows from SQLite and publish CoT XML
#     to a TAK server over TLS. Marker type, group tag, interval, UID salt
#     and dual-publish are controlled via environment.
# Notes:
#   - Visual status (PREFLIGHT/AIRBORNE/etc.) is computed here using AGL and
#     last_position_utc. This logic can be moved to a shared util or persisted
#     if you prefer it outside the publisher.
# ---------------------------------------------------------------------------

import os, ssl, asyncio, threading, logging, sqlite3
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape
from pathlib import Path
from typing import Optional, Dict

log = logging.getLogger("cot")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# Resolve DB path from config if available
try:
    from ..config import DB_PATH as _CFG_DB_PATH
    DB_PATH = Path(_CFG_DB_PATH)
except Exception:
    DB_PATH = Path("tracking_data/device_latest.db")

# Optional visual config
CALLSIGN_STATIC = os.getenv("COT_CALLSIGN_STATIC", "SR00").strip() or "SR00"
ICONSET_PATH = os.getenv("COT_ICONSET_PATH", "").strip()  # e.g., "User Icons"
ICON_FILE = os.getenv("COT_ICON_FILE", "").strip()        # e.g., "saber.png"
# Marker type: use simple point to allow custom colors
MARKER_TYPE = os.getenv("COT_MARKER_TYPE", "b-m-p-s").strip() or "b-m-p-s"
# Publish interval seconds
PUBLISH_INTERVAL_SEC = float(os.getenv("COT_PUBLISH_INTERVAL_SEC", "60").strip() or 60)

# Optional group tagging for TAK dissemination
GROUP_NAME = os.getenv("COT_GROUP_NAME", "").strip()
GROUP_ROLE = os.getenv("COT_GROUP_ROLE", "").strip()
UID_SALT = os.getenv("COT_UID_SALT", "").strip()

# Optional dual-publish (e.g., also send a MIL-STD marker type for visibility)
def _env_bool(name: str, default: str = "") -> bool:
    return str(os.getenv(name, default)).strip().lower() not in ("", "0", "false", "no", "off")

DUAL_MARKER = _env_bool("COT_DUAL_MARKER", "")
DUAL_TYPE = os.getenv("COT_DUAL_TYPE", "a-f-G-U-C").strip() or "a-f-G-U-C"

# Terrain provider (optional; graceful fallback if missing)
SRTM_CACHE_DIR = os.getenv("SRTM_CACHE_DIR", str(Path("tracking_data/srtm").resolve()))
try:
    import srtm  # type: ignore
    _srtm_data = srtm.get_data(local_cache_dir=SRTM_CACHE_DIR)
except Exception:
    srtm = None
    _srtm_data = None

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

def _age_minutes(iso_ts: str | None) -> float:
    if not iso_ts or not isinstance(iso_ts, str):
        return 1e9
    try:
        ts = iso_ts.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60.0)
    except Exception:
        return 1e9

def _select_color(*, status: str) -> str:
    s = (status or "").upper()
    if s == "PREFLIGHT":
        return "Green"
    if s == "AIRBORNE":
        return "Cyan"
    if s == "TERMINATED":
        return "Red"
    if s == "LANDED":
        return "Brown"
    if s == "ABANDONED":
        return "Black"
    if s == "PRODUCTION":
        return "Magenta"  # purple-ish
    return "Green"

def _float_or_default(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)

def _argb(color_name: str) -> int:
    # Android ARGB ints for common colors
    cmap = {
        "Black": -16777216,
        "Red": -65536,
        "Green": -16711936,
        "Cyan": -16711681,
        "Brown": -12042869,  # approx #8B4513
        "Yellow": -256,
        "White": -1,
        "Blue": -16776961,
        "Magenta": -65281,
    }
    return cmap.get(color_name, -1)

def _to_opt_float(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _build_cot_xml(*, device_id: str, lat: float, lon: float, alt_m: float,
                   utc_time: str, local_date: str, local_time: str, display_status: str,
                   callsign: str, last_pos_iso: str,
                   balloon_type: Optional[str] = None,
                   max_alt_m: Optional[float] = None,
                   agl_m: Optional[float] = None, ground_m: Optional[float] = None,
                   marker_type: Optional[str] = None) -> str:
    now = datetime.now(timezone.utc)
    time_s = _iso(now)
    start_s = time_s
    stale_s = _iso(now + timedelta(minutes=2))

    # sanitize
    mtype = escape(marker_type or MARKER_TYPE)
    is_milstd = (marker_type or MARKER_TYPE).startswith("a-")
    # Force a fresh symbol render on clients when switching to MIL-STD by suffixing UID once
    uid = f"{device_id}-ms" if is_milstd else device_id
    if UID_SALT:
        uid = f"{uid}-{UID_SALT}"
    did = escape(uid or "")
    # Build requested Remarks lines
    # 1) Status
    # 2) Last report (UTC) dd MMM YY HH:MM
    # 3) Altitude (ft)
    # 4) Latitude DD.DDDD
    # 5) Longitude DDD.DDDD
    # 6) Balloon type
    try:
        dt_last = datetime.fromisoformat((last_pos_iso or "").replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        dt_last = datetime.now(timezone.utc)
    last_line = dt_last.strftime("%d %b %y %H:%M")
    alt_ft = int(round(_float_or_default(alt_m, 0.0) * 3.28084))
    max_alt_ft = int(round(_float_or_default(max_alt_m, 0.0) * 3.28084)) if max_alt_m is not None else 0
    remarks_txt = (
        f"Status: {display_status}\n"
        f"Last report: {last_line} UTC\n"
        f"Altitude: {alt_ft} ft\n"
        f"Latitude: {lat:.4f}\n"
        f"Longitude: {lon:.4f}\n"
        f"Balloon type: {balloon_type or ''}\n"
        f"Max Altitude: {max_alt_ft} ft MSL"
    )
    remarks = escape(remarks_txt)
    color = escape(_select_color(status=display_status))

    # ce/le large to indicate unknown accuracy (common practice)
    # hae = altitude above ellipsoid (we'll send meters we have)
    extra_parts = []
    if ICONSET_PATH and ICON_FILE:
        extra_parts.append(f'<usericon iconsetpath="{escape(ICONSET_PATH)}" icon="{escape(ICON_FILE)}"/>')
    if GROUP_NAME:
        role_attr = f' role="{escape(GROUP_ROLE)}"' if GROUP_ROLE else ''
        extra_parts.append(f'<__group name="{escape(GROUP_NAME)}"{role_attr}/>')
    extra = ''.join(extra_parts)
    # Pick reasonable CE/LE to avoid giant uncertainty circles on MIL-STD
    ce_val = 20 if is_milstd else 9999999
    le_val = 20 if is_milstd else 9999999
    parts = [
        f'<event version="2.0" type="{mtype}" uid="{did}" ',
        f'time="{time_s}" start="{start_s}" stale="{stale_s}" how="h-g-i-g-o">',
        f'<point lat="{lat:.6f}" lon="{lon:.6f}" hae="{alt_m:.1f}" ce="{ce_val}" le="{le_val}"/>',
        f'<detail>',
        f'<contact callsign="{escape(callsign)}"/>',
        f'<remarks>{remarks}</remarks>',
    ]
    if not is_milstd:
        parts += [
            f'<color>{color}</color>',
            f'<strokeColor>{_argb(color)}</strokeColor>',
            f'<fillColor>{_argb(color)}</fillColor>',
            f'<strokeWeight>2</strokeWeight>',
        ]
    if extra:
        parts.append(extra)
    parts += [f'</detail>', f'</event>']
    xml = ''.join(parts)
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

    # Build query; include sr_num/callsign/flight_started if present
    include_sr = False
    has_callsign = False
    has_flight_started = False
    has_balloon_type = False
    has_visual_status = False
    has_max_alt = False
    with sqlite3.connect(DB_PATH) as _c:
        try:
            cols = [r[1] for r in _c.execute("PRAGMA table_info(device_latest)")]
            include_sr = "sr_num" in cols
            has_callsign = "callsign" in cols
            has_flight_started = "flight_started" in cols
            has_balloon_type = "balloon_type" in cols
            has_visual_status = "status" in cols
            has_max_alt = "max_alt_m" in cols
        except Exception:
            include_sr = False
            has_callsign = False
            has_flight_started = False
            has_balloon_type = False
            has_visual_status = False
            has_max_alt = False

    cols_select = [
        "device_id", "utc_time", "local_date", "local_time",
        "lat", "lon", "alt_m", "status", "last_position_utc"
    ]
    if include_sr:
        cols_select.append("sr_num")
    if has_callsign:
        cols_select.append("callsign")
    if has_flight_started:
        cols_select.append("flight_started")
    if has_balloon_type:
        cols_select.append("balloon_type")
    if has_max_alt:
        cols_select.append("max_alt_m")
    # (balloon_type exists but not used in publisher yet)
    # Optional status filter (env):
    #   COT_STATUS_FILTER=all (default) or not_abandoned
    status_filter = os.getenv("COT_STATUS_FILTER", "all").strip().lower()
    base_query = f"SELECT {', '.join(cols_select)} FROM device_latest"
    if status_filter == "not_abandoned":
        query = base_query + " WHERE COALESCE(status,'') != 'ABANDONED'"
    else:
        query = base_query
    query += " ORDER BY device_id"

    while True:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()

            for row in rows:
                idx = 0
                device_id, utc_time, local_date, local_time, lat, lon, alt_m, status, last_pos_utc = row[:9]
                idx = 9
                sr_num = row[idx] if include_sr else None; idx += 1 if include_sr else 0
                callsign_str = row[idx] if has_callsign else None; idx += 1 if has_callsign else 0
                flight_started_db = row[idx] if has_flight_started else None; idx += 1 if has_flight_started else 0
                balloon_type = row[idx] if has_balloon_type else None; idx += 1 if has_balloon_type else 0
                max_alt_m = row[idx] if has_max_alt else None
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

                # Callsign: SR## from sr_num if available; else explicit callsign; else static
                cs = CALLSIGN_STATIC
                try:
                    if sr_num is not None:
                        cs = f"SR{int(sr_num):02d}"
                    elif callsign_str:
                        cs = str(callsign_str).strip() or cs
                except Exception:
                    pass

                # Terrain-based status computation
                ground = _get_ground_elevation(lat, lon)
                agl = None
                if ground is not None:
                    agl = max(0.0, alt_m - ground)

                # Prefer persisted status; fall back to compute if missing
                display_status = str(status or "") or _compute_visual_status(
                    status_db=str(status or ""),
                    agl_m=agl,
                    last_pos_iso=str(last_pos_utc or ""),
                    device_id=str(device_id),
                    started_persisted=bool(flight_started_db) if has_flight_started else False,
                    production=False,
                )

                # Persist flight_started once we see airborne and column exists
                if has_flight_started and display_status == "AIRBORNE" and not bool(flight_started_db):
                    try:
                        with sqlite3.connect(DB_PATH) as c2:
                            c2.execute("UPDATE device_latest SET flight_started=1 WHERE device_id=?", (device_id,))
                            c2.commit()
                    except Exception:
                        pass

                xml = _build_cot_xml(
                    device_id=device_id, lat=lat, lon=lon, alt_m=alt_m,
                    utc_time=utc_time or "", local_date=local_date or "", local_time=local_time or "",
                    display_status=display_status, callsign=cs, last_pos_iso=str(last_pos_utc or ""),
                    balloon_type=(str(balloon_type) if balloon_type is not None else None),
                    max_alt_m=_to_opt_float(max_alt_m),
                    agl_m=agl, ground_m=ground,
                    marker_type=MARKER_TYPE,
                )

                writer.write((xml + "\n").encode("utf-8"))
                if DUAL_MARKER:
                    xml2 = _build_cot_xml(
                        device_id=device_id, lat=lat, lon=lon, alt_m=alt_m,
                        utc_time=utc_time or "", local_date=local_date or "", local_time=local_time or "",
                        display_status=display_status, callsign=cs, last_pos_iso=str(last_pos_utc or ""),
                        balloon_type=(str(balloon_type) if balloon_type is not None else None),
                        max_alt_m=_to_opt_float(max_alt_m),
                        agl_m=agl, ground_m=ground,
                        marker_type=DUAL_TYPE,
                    )
                    writer.write((xml2 + "\n").encode("utf-8"))
                await writer.drain()
                log.info("[cot] published event uid=%s vis=%s lat=%.6f lon=%.6f alt=%.1f agl=%s",
                         device_id, display_status, lat, lon, alt_m, (f"{agl:.1f}" if agl is not None else "?"))

        except Exception as e:
            log.exception("[cot] publish loop error: %s", e)

        # send updates per interval
        await asyncio.sleep(PUBLISH_INTERVAL_SEC)

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

# -----------------------------
# Terrain + status helpers
# -----------------------------

_flight_seen: Dict[str, bool] = {}

def _get_ground_elevation(lat: float, lon: float) -> Optional[float]:
    try:
        if _srtm_data is not None:
            h = _srtm_data.get_elevation(lat, lon)
            if h is None:
                return None
            return float(h)
    except Exception:
        return None
    return None

def _compute_visual_status(*, status_db: str, agl_m: Optional[float], last_pos_iso: str, device_id: str,
                           started_persisted: bool, production: bool) -> str:
    # Terminated shell (not active yet): honor explicit DB status if already set
    sdb = (status_db or "").upper()
    if production:
        return "PRODUCTION"
    if sdb == "TERMINATED":
        return "TERMINATED"

    # Age
    age_min = _age_minutes(last_pos_iso)

    # Flight state memory per run
    started = _flight_seen.get(device_id, False) or bool(started_persisted)
    if isinstance(agl_m, (int, float)):
        if agl_m >= 100.0:
            _flight_seen[device_id] = True
            return "AIRBORNE"
        # AGL < 100
        if started:
            if age_min >= 24 * 60:
                return "ABANDONED"
            return "LANDED"
        else:
            return "PREFLIGHT"

    # Fallbacks when no terrain data
    if sdb == "IN_FLIGHT":
        _flight_seen[device_id] = True
        return "AIRBORNE"
    if age_min >= 24 * 60:
        return "ABANDONED"
    return "PREFLIGHT"

def start_cot_publisher():
    t = threading.Thread(target=_runner, name="cot-publisher", daemon=True)
    t.start()
