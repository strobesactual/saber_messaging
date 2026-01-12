# app/decoding/payload_decoder.py
# ---------------------------------------------------------------------------
# Responsibility:
#   - Decode fixed 25-byte payloads into normalized fields. Tolerant of short
#     payloads and clamps out-of-range lat/lon. Computes UTC/local time strings.
# ---------------------------------------------------------------------------
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
    """
    Decode as much of the fixed-layout payload as is available.
    Any missing fields fall back to "" so callers still receive a structured result.
    """
    raw = raw or b""

    def _read_u32(offset: int) -> int | None:
        chunk = raw[offset:offset + 4]
        if len(chunk) == 4:
            return int.from_bytes(chunk, "big", signed=False)
        return None

    burn = raw[0] if len(raw) >= 1 else None  # kept for completeness / future use

    time_u32 = _read_u32(1)
    lat_u32 = _read_u32(5)
    lon_u32 = _read_u32(9)
    alt_u32 = _read_u32(13)
    temp_u32 = _read_u32(17)
    pres_u32 = _read_u32(21)

    lat = round(lat_u32 / 1e5 - 90.0, 6) if lat_u32 is not None else None
    if lat is not None and not (-90.0 <= lat <= 90.0):
        lat = None
    lon = round(lon_u32 / 1e5 - 180.0, 6) if lon_u32 is not None else None
    if lon is not None and not (-180.0 <= lon <= 180.0):
        lon = None
    # Flight software encodes altitude as (alt_m + 200) * 100 to keep it positive.
    alt_m = round((alt_u32 / 100.0) - 200.0, 1) if alt_u32 is not None else None
    alt_ft = round(alt_m * 3.28084, 2) if alt_m is not None else None
    temp_k = round(temp_u32 / 100.0, 2) if temp_u32 is not None else None
    temp_c = round(temp_k - 273.15, 2) if temp_k is not None else None
    pressure_hpa = round(pres_u32 / 100.0, 2) if pres_u32 is not None else None

    utc_hms = _hhmmss_from_cc(time_u32) if time_u32 is not None else ""
    utc_dt = None
    if utc_hms:
        utc_today = datetime.now(timezone.utc).replace(microsecond=0)
        try:
            utc_dt = utc_today.replace(
                hour=int(utc_hms[0:2]),
                minute=int(utc_hms[3:5]),
                second=int(utc_hms[6:8])
            )
        except Exception:
            utc_hms = ""
            utc_dt = None

    local_dt = None
    if utc_dt is not None:
        if lat is not None and lon is not None and _tzf is not None:
            try:
                tzname = _tzf.timezone_at(lng=lon, lat=lat)
                if tzname:
                    local_dt = utc_dt.astimezone(ZoneInfo(tzname))
            except Exception:
                local_dt = None
        if local_dt is None and lon is not None:
            try:
                offset_hours = round(lon / 15)
                if offset_hours < -12 or offset_hours > 14:
                    offset_hours = 0
                local_dt = utc_dt.astimezone(timezone(timedelta(hours=offset_hours)))
            except Exception:
                local_dt = None
        if local_dt is None:
            local_dt = utc_dt

    return {
        "device_id": "",
        "lat": lat if lat is not None else "",
        "lon": lon if lon is not None else "",
        "alt_m": alt_m if alt_m is not None else "",
        "alt_ft": alt_ft if alt_ft is not None else "",
        "temp_k": temp_k if temp_k is not None else "",
        "temp_c": temp_c if temp_c is not None else "",
        "pressure_hpa": pressure_hpa if pressure_hpa is not None else "",
        "utc_time": utc_hms,
        "local_date": local_dt.strftime("%d %b %y") if local_dt is not None else "",
        "local_time": local_dt.strftime("%H:%M:%S") if local_dt is not None else "",
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
    return _parse_fixed_payload(raw[:25])

def decode_b64(payload_b64: str) -> dict:
    raw = base64.b64decode(payload_b64 or "")
    return _parse_fixed_payload(raw[:25])
