import CoT
import datetime
import socket
import time

# Current time
now = datetime.datetime.now(datetime.timezone.utc)
stale = now + datetime.timedelta(minutes=2)

# Test CoT event for Topeka, KS
test_cot = CoT.Event(
    version="2.0",
    type="a-f-G-U-C",
    uid="test-topeka-002",
    time=now,
    start=now,
    stale=stale,
    how="h-g-i-g-o",
    point=CoT.Point(lat=39.03, lon=-95.68, hae=10.0, ce=9999999, le=9999999),
    detail={"contact": {"callsign": "test-topeka-002"}}
)

# TAK server details
TAK_IP = "192.168.1.62"
TAK_PORT = 8089

# Send via TCP with debug
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.connect((TAK_IP, TAK_PORT))
    print(f"Connected to {TAK_IP}:{TAK_PORT}")
    sock.sendall(bytes(test_cot.xml(), encoding="utf-8"))
    print(f"Sent CoT XML: {test_cot.xml()}")
    time.sleep(5)  # Wait 5 seconds to allow processing
except Exception as e:
    print(f"Error: {e}")
finally:
    sock.close()
    print("Socket closed")

print(f"Sent test CoT for test-topeka-002 to {TAK_IP}:{TAK_PORT}")
