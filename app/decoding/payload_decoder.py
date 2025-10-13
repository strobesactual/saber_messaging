# app/decoding/payload_decoder.py
# =============================================================================
# Payload Decoding (Kyberdyne fixed layout v2, 25 bytes)
# -----------------------------------------------------------------------------
# Fixed layout mapping (big-endian; 25 bytes total):
#      [0]      burn/version (u8)
#      [1:5]    time_u32  (HHMMSS00) -> HH:MM:SS
#      [5:9]    lat_u32   -> (raw/1e5) - 90.0         -> DD.DDDDDD
#      [9:13]   lon_u32   -> (raw/1e5) - 180.0        -> DD.DDDDDD
#      [13:17]  alt_u32   -> meters = raw/100
#      [17:21]  temp_u32  -> Kelvin = raw/100
#      [21:25]  pres_u32  -> hPa    = raw/100
#
# Time handling:
#   - If the payload includes HHMMSS, we compute local time via real TZ where possible, else lon/15 fallback.
#   - If payload lacks time, we fall back to envelope unixTime (GPS epoch) minus 18s to UTC.
#
# Output precision:
#   - lat/lon: 6 decimals; alt_m: 0.1 m; alt_ft: 2 decimals; temp/pressure: 2 decimals.
# =============================================================================

import base64, binascii
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

try:
    from timezonefinder import TimezoneFinder
    _tzf = TimezoneFinder()
except Exception:
    _tzf = None

def _hhmmss_from_cc(raw_u32: int) -> str:
    s = f"{raw_u32:08d}"[-8:]
    return f"{s[0:2]}:{s[2:4]}:{s[4:6]}"

def _parse_fixed_payload(raw: bytes) -> dict:
    # 25-byte v2 layout
    if len(raw) < 25:
        raise ValueError(f"payload too short for fixed layout v2: {len(raw)} bytes")

    burn      = raw[0]
    time_u32  = int.from_bytes(raw[1:5],   "big", signed=False)
    lat_u32   = int.from_bytes(raw[5:9],   "big", signed=False)
    lon_u32   = int.from_bytes(raw[9:13],  "big", signed=False)
    alt_u32   = int.from_bytes(raw[13:17], "big", signed=False)
    temp_u32  = int.from_bytes(raw[17:21], "big", signed=False)
    pres_u32  = int.from_bytes(raw[21:25], "big", signed=False)

    lat    = round(lat_u32 / 1e5 - 90.0, 6)
    lon    = round(lon_u32 / 1e5 - 180.0, 6)
    alt_m  = round(alt_u32 / 100.0, 1)
    alt_ft = round(alt_m * 3.28084, 2)
    temp_k = round(temp_u32 / 100.0, 2)
    pressure_hpa = round(pres_u32 / 100.0, 2)

    utc_hms = _hhmmss_from_cc(time_u32)

    # Build a UTC datetime "today" for HH:MM:SS
    utc_today = datetime.now(timezone.utc).replace(microsecond=0)
    utc_dt = utc_today.replace(
        hour=int(utc_hms[0:2]),
        minute=int(utc_hms[3:5]),
        second=int(utc_hms[6:8])
    )

    # Prefer real TZ (DST-aware); fallback lon/15
    local_dt = None
    if _tzf is not None:
        try:
            tzname = _tzf.timezone_at(lng=lon, lat=lat)
            if tzname:
                local_dt = utc_dt.astimezone(ZoneInfo(tzname))
        except Exception:
            local_dt = None
    if local_dt is None:
        try:
            offset_hours = round(lon / 15)
            if offset_hours < -12 or offset_hours > 14:
                offset_hours = 0
            local_dt = utc_dt.astimezone(timezone(timedelta(hours=offset_hours)))
        except Exception:
            local_dt = utc_dt

    return {
        "device_id": "",
        "lat": lat, "lon": lon,
        "alt_m": alt_m, "alt_ft": alt_ft,
        "temp_k": temp_k, "pressure_hpa": pressure_hpa,
        "utc_time": utc_hms,
        "local_date": local_dt.strftime("%d %b %y"),
        "local_time": local_dt.strftime("%H:%M:%S"),
        "raw": raw.hex()
    }

def decode_from_hexstring(hex_text: str) -> dict:
    cleaned = (hex_text or "").strip()
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]
    if len(cleaned) % 2 != 0:
        cleaned = "0" + cleaned
    try:
        raw = binascii.unhexlify(cleaned)
    except binascii.Error as e:
        raise ValueError(f"invalid hex payload: {e}")
    if len(raw) >= 25:
        return _parse_fixed_payload(raw[:25])
    return {
        "device_id": "",
        "lat": "", "lon": "",
        "alt_m": "", "alt_ft": "",
        "temp_k": "", "pressure_hpa": "",
        "utc_time": "", "local_date": "", "local_time": "",
        "raw": raw.hex()
    }

def decode_b64(payload_b64: str) -> dict:
    raw = base64.b64decode(payload_b64 or "")
    if len(raw) >= 25:
        return _parse_fixed_payload(raw[:25])
    return {
        "device_id": "",
        "lat": "", "lon": "",
        "alt_m": "", "alt_ft": "",
        "temp_k": "", "pressure_hpa": "",
        "utc_time": "", "local_date": "", "local_time": "",
        "raw": raw.hex()
    }
