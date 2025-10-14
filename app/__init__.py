# app/__init__.py

from flask import Flask
import os
from .api import register_routes
from .storage import device_index
from . import record_messages
from .process_messages import set_tracker
from .cot import start_cot_publisher

_cot_started = False


def _maybe_start_cot():
    global _cot_started
    if _cot_started:
        return
    cot_url = os.getenv("COT_URL", "").strip()
    if not cot_url:
        return
    start_cot_publisher()
    _cot_started = True


def create_app():
    app = Flask(__name__)

    # ensure process_messages updates the in-memory index
    set_tracker(device_index)
    _maybe_start_cot()

    register_routes(app, device_index, record_messages)
    return app
