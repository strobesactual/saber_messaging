# app/process_messages.py
# -----------------------------------------------------------------------------
# Responsibility:
#   - Convert one inbound message into a normalized observation.
#   - Detect payload encoding, decode, derive status/timestamps, persist.
# Entry point:
#   - process_incoming(body_dict) called by app/api.py
# Notes:
#   - Status here is altitude-based (coarse). Visual status (AGL-based) is
#     currently computed in the CoT publisher; can be refactored later.
# -----------------------------------------------------------------------------

from __future__ import annotations

import base64
import binascii
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .decoding import payload_decoder
from .record_messages import record_observation

logger = logging.getLogger("ingest")

# Optional in-memory tracker (DeviceIndex-like) the web app can register.
_TRACKER = None  # type: Optional[Any]


def set_tracker(tracker_like: Any) -> None:
    """
    Optionally register a tracker that exposes:
        update(observation_dict) and/or upsert(observation_dict)
    """
    global _TRACKER
    _TRACKER = tracker_like


# -----------------------
# Helpers / normalizers
# -----------------------
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _looks_hex(s: str) -> bool:
    t = s.strip().lower()
    if t.startswith("0x"):
        t = t[2:]
    if len(t) == 0 or len(t) % 2 != 0:
        return False
    try:
        int(t, 16)
        return True
    except ValueError:
        return False


def _decode_payload(payload: str, encoding: Optional[str]) -> Dict[str, Any]:
    """
    Returns a dict from payload_decoder with keys:
      lat, lon, alt_m, alt_ft, temp_k, temp_c, pressure_hpa, utc_time, local_date, local_time, raw
    """
    if not isinstance(payload, str) or not payload.strip():
        raise ValueError("payload must be a non-empty string")

    enc = (encoding or "").strip().lower()
    if enc in ("hex", "hexstring"):
        return payload_decoder.decode_from_hexstring(payload)
    if enc in ("b64", "base64"):
        return payload_decoder.decode_b64(payload)

    # Auto-detect if not provided
    if _looks_hex(payload):
        return payload_decoder.decode_from_hexstring(payload)
    else:
        # if it's not valid b64, payload_decoder.decode_b64 will raise
        return payload_decoder.decode_b64(payload)


def _compute_status(alt_m: Optional[float]) -> str:
    if alt_m is None:
        return "UNKNOWN"
    try:
        if float(alt_m) > 50.0:
            return "IN_FLIGHT"
        return "ON_GROUND"
    except Exception:
        return "UNKNOWN"


def _valid_latlon(lat: Any, lon: Any) -> bool:
    try:
        lat = float(lat)
        lon = float(lon)
        return (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0)
    except Exception:
        return False


def _last_position_iso(envelope_iso: Optional[str], utc_hms: str) -> str:
    """
    Pick the best available timestamp for ordering messages. Prefer explicit
    envelope_time_iso; otherwise synthesize an ISO string from the payload UTC
    time on today's date (UTC). Fallback to now if payload time is unusable.
    """
    if isinstance(envelope_iso, str) and envelope_iso.strip():
        return envelope_iso.strip()
    if isinstance(utc_hms, str) and len(utc_hms) >= 8 and utc_hms[2] == ":" and utc_hms[5] == ":":
        try:
            today = datetime.now(timezone.utc).replace(
                hour=int(utc_hms[0:2]),
                minute=int(utc_hms[3:5]),
                second=int(utc_hms[6:8]),
                microsecond=0,
            )
            return today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            pass
    return _utcnow_iso()


# -----------------------
# Main entry point
# -----------------------
def process_incoming(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected body fields:
      - device_id (str)  [required]
      - payload  (str)   [required] raw message (b64 or hex)
      - encoding (str)   [optional] 'base64'/'b64' or 'hex'
      - envelope_time_iso (str) [optional] external time to stamp last_position_utc (ISO 8601)

    Returns: {"status":"success","device_id": "..."} or {"status":"error","error":"..."}.
    """
    try:
        device_id = str(body.get("device_id", "")).strip()
        if not device_id:
            raise ValueError("device_id is required")

        payload = body.get("payload")
        if not isinstance(payload, str) or not payload.strip():
            raise ValueError("payload is required")

        encoding = body.get("encoding")
        decoded = _decode_payload(payload, encoding)

        # Ignore noise frames that start with a 0x00 header (seen from nearby non-balloon devices).
        raw_hex = (decoded.get("raw") or "").strip().lower()
        if raw_hex.startswith("00"):
            logger.info("ignoring device=%s raw startswith 0x00 (likely non-balloon chatter)", device_id)
            return {"status": "success", "device_id": device_id, "ignored": True}
        # Enforce expected header: only accept payloads starting with 0x02.
        if raw_hex and not raw_hex.startswith("02"):
            logger.info("ignoring device=%s raw missing 0x02 header: %s", device_id, raw_hex[:8])
            return {"status": "success", "device_id": device_id, "ignored": True}

        # Normalize observation fields
        obs: Dict[str, Any] = dict(decoded)  # copy
        obs["device_id"] = device_id
        # Optional correlation id (for dedup of CSV on BOF retries)
        corr = body.get("correlation_id")
        if isinstance(corr, str) and corr.strip():
            obs["correlation_id"] = corr.strip()

        # Position timestamp: prefer envelope_time_iso, else synthesize from payload UTC time
        last_pos_iso = _last_position_iso(body.get("envelope_time_iso"), decoded.get("utc_time", ""))
        obs["last_position_utc"] = last_pos_iso

        # Derived status
        obs["status"] = _compute_status(obs.get("alt_m"))

        # Persist all outputs (CSV, GeoJSON, KML, SQLite upsert)
        updated = record_observation(obs)

        # Optionally sync to in-memory tracker for fast reads
        if _TRACKER is not None and updated:
            try:
                if hasattr(_TRACKER, "update"):
                    _TRACKER.update(obs)
                elif hasattr(_TRACKER, "upsert"):
                    _TRACKER.upsert(obs)
            except Exception as te:
                logger.warning("tracker update failed: %s", te)

        logger.info("ingested device=%s lat=%.6f lon=%.6f alt_m=%s status=%s",
                    device_id,
                    (obs.get("lat") or 0.0),
                    (obs.get("lon") or 0.0),
                    obs.get("alt_m"),
                    obs.get("status"))

        return {"status": "success", "device_id": device_id}

    except (binascii.Error, base64.binascii.Error) as e:
        # decoding errors
        logger.error("decode error: %s", e)
        return {"status": "error", "error": f"decode_error: {e}"}
    except Exception as e:
        logger.exception("ingest error")
        return {"status": "error", "error": str(e)}


__all__ = ["process_incoming", "set_tracker"]
