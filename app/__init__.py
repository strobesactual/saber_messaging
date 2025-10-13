# app/__init__.py
from __future__ import annotations
import logging, os
from flask import Flask

def _setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

def create_app() -> Flask:
    _setup_logging()

    # Local imports to avoid side effects during package import
    from .config import settings
    from .storage import record_messages as rec
    from .storage import device_index as tracker
    from .api import register_routes
    from .cot.cot_publisher import start_cot_publisher

    app = Flask(__name__)

    # Ensure outputs exist & warm in-memory index
    rec.ensure_directories()
    try:
        tracker.warm_start(rec.CSV_FILE)
    except Exception as e:
        print(f"[tracker] warm_start failed: {e}")

    register_routes(app, tracker, rec)

    # Start CoT publisher thread only if env is set (safe inside function)
    start_cot_publisher()

    return app

__all__ = ["create_app"]
