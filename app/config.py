# app/config.py
import os

class Settings:
    TRACKING_DIR = os.getenv("TRACKING_DIR", "tracking_data")
    # TAK / PyTAK related envs are read directly by cot_publisher.py

settings = Settings()
