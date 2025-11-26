#!/usr/bin/env python
"""
Keeps the CoT publisher running as a long-lived process.
Intended for systemd (see saber-cot.service).
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from app.cot import start_cot_publisher


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop_evt = threading.Event()

    def _handle(sig, frame):  # type: ignore[override]
        logging.info("received signal %s, stopping", sig)
        stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle)
        except Exception:
            pass

    start_cot_publisher()
    logging.info("CoT publisher started; entering keepalive loop.")
    try:
        while not stop_evt.is_set():
            stop_evt.wait(60)
    except KeyboardInterrupt:
        stop_evt.set()
    logging.info("CoT publisher exiting.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
