# app/cot/cot_runner.py

from cot_publisher import start_cot_publisher
start_cot_publisher()
import time
while True: time.sleep(3600)
