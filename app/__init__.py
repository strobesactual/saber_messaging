# app/__init__.py
# ---------------------------------------------------------------------------
# Responsibility:
#   - Flask app factory. Wires routes, device index, and starts the CoT
#     publisher thread if COT_URL is set in the environment.
# ---------------------------------------------------------------------------

from flask import Flask
import os
from .api import register_routes
from .storage import device_index
from . import record_messages
from .process_messages import set_tracker
from .cot import start_cot_publisher
from . import config as cfg

_cot_started = False


def _maybe_start_cot():
    global _cot_started
    if _cot_started:
        return
    if os.getenv("COT_DISABLED", "").strip():
        return

    cot_url = os.getenv("COT_URL", "").strip() or getattr(cfg, "COT_URL", "").strip()
    if not cot_url:
        return
    # ensure downstream modules see the resolved URL
    os.environ.setdefault("COT_URL", cot_url)
    start_cot_publisher()
    _cot_started = True


def create_app():
    app = Flask(__name__)

    # ensure process_messages updates the in-memory index
    set_tracker(device_index)
    _maybe_start_cot()

    register_routes(app, device_index, record_messages)
    return app
