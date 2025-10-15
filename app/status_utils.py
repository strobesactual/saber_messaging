# app/status_utils.py
# ---------------------------------------------------------------------------
# Responsibility:
#   - Shared helpers for computing visual status for display/publishing.
#     Logic mirrors the CoT publisher but is usable from persistence.
# ---------------------------------------------------------------------------

from __future__ import annotations
import os
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path

# Optional SRTM for AGL (graceful fallback if unavailable)
SRTM_CACHE_DIR = os.getenv("SRTM_CACHE_DIR", str(Path("tracking_data/srtm").resolve()))
try:
    import srtm  # type: ignore
    _srtm_data = srtm.get_data(local_cache_dir=SRTM_CACHE_DIR)
except Exception:
    _srtm_data = None


def _age_minutes(iso_ts: str | None) -> float:
    if not iso_ts or not isinstance(iso_ts, str):
        return 1e9
    try:
        ts = iso_ts.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60.0)
    except Exception:
        return 1e9


def _get_ground_elevation(lat: Optional[float], lon: Optional[float]) -> Optional[float]:
    try:
        if _srtm_data is None or lat is None or lon is None:
            return None
        h = _srtm_data.get_elevation(float(lat), float(lon))
        if h is None:
            return None
        return float(h)
    except Exception:
        return None


def compute_visual_status(
    *,
    lat: Optional[float],
    lon: Optional[float],
    alt_m: Optional[float],
    last_position_utc: Optional[str],
    flight_started: bool,
) -> str:
    """
    Returns one of: PREFLIGHT, AIRBORNE, LANDED, ABANDONED
    Rules (AGL-aware when possible):
      - If AGL >= 100 m → AIRBORNE (and flight_started becomes true upstream)
      - Else if flight_started and age >= 24h → ABANDONED
      - Else if flight_started → LANDED
      - Else → PREFLIGHT
    """
    try:
        agl = None
        if alt_m is not None and lat is not None and lon is not None:
            g = _get_ground_elevation(lat, lon)
            if g is not None:
                agl = max(0.0, float(alt_m) - g)

        if agl is not None:
            if agl >= 100.0:
                return "AIRBORNE"

        age_min = _age_minutes(last_position_utc)
        if flight_started:
            if age_min >= 24 * 60:
                return "ABANDONED"
            return "LANDED"
        return "PREFLIGHT"
    except Exception:
        return "PREFLIGHT"

