# saber_messaging

Purpose:  
   - Receive Globalstar messages (XML/hex or JSON/Base64)
   - Decode lat/lon/alt/timestamps robustly
   - Persist points to CSV, KML, and GeoJSON
   - Serve read-only endpoints for downstream tools and quick live views

Globalstar Back Office (BOF) interface key points you MUST honor:  
   - HTTP 1.1 POSTs arrive with Accept: text/xml and Content-Type: text/xml (NOT application/xml).
   - You must return HTTP/200 with a well-formed stuResponseMsg/prvResponseMsg XML body.
   - BOF may include both <stuMessage> and <ackMessage> in a batch; we ignore ackMessage.
   - Known BOF egress IPs to allowlist at the gateway: 3.228.87.237, 34.231.245.76, 3.135.136.171, 3.133.245.206
   - XSDs referenced in responses: http://cody.glpconnect.com/XSD/StuResponse_Rev1_0.xsd

 Notes on output formats consumed downstream:
   - CSV headers fixed; append-only.
   - KML 2.2 (OGC) with one Placemark per point; coords order lon,lat,alt (meters).
   - GeoJSON RFC 7946 FeatureCollection; geometry Point [lon, lat, alt_m]; properties carry meta.
   - Coordinates preferred precision: DD.DDDDDD (6 decimal places).

## **Architecture**

SABER communicates via Globalstar messages:
1) The balloon will send a message to a globalstar satellite
2) The satellite will relay that message back down to a Globalstar server on Earth
3) The Globalstar server will then push the message to Kyberdyne's SABER Server
4) The SABER Server will decode, parse, and store the message contents

You cannot query the Globalstar server, rather, messages are sent from Globalstar to a static IP address. To manage this, email Thomas Babb <Thomas.Babb@globalstar.com> 

Messages should be formatted thus:  
- Method: POST  
- URL: http://kyberdyne.ddns.net:5050/  
- Headers: Content-Type: text/xml Accept: text/xml  
- Timeout: ≥ 15 s  
- Expected response: HTTP 200 with an XML body like <stuResponseMsg>…<state>pass</state>…</stuResponseMsg>  

## **Formatting**
The total length of the messages can be larger or smaller, but it’s solely dependent upon how much data we add to them. Each piece of data will always be the same length—4 bytes, or 8 hex characters. Here’s how it works:

Data is collected on the TBeam. For the software I’m currently running, that data is (in order) lat, lng, alt, and time. So, 4 pieces of data. 

Each of those pieces of data is converted into a 32-bit binary number. Those are then each converted into a 4-byte hex number—simply turning it from 1s and 0s into 0-F. So we then have 4 hex numbers, each are 4 bytes, and a byte in hex is represented by 2 hex characters, which means each piece of data is 8 characters long. That number is currently always true: any piece of data that is sent from the t-beam is contained in 8 hex characters. 

I always add one byte (2 hex characters) in front of the message, because when global star sends messages, they flip the first byte for some reason. So the first byte that is in the message is useless. 

So, with 4 pieces of data each composed of 8 characters, plus 2 characters (1 byte) in the front to account for global star’s bit flipping, we have a message length of 2 + 8*4, or 34. 

<img width="703" height="71" alt="image" src="https://github.com/user-attachments/assets/271db5eb-4246-40e1-b091-821dcb657cdb" />

Base64: AgD5YpQAw2aUAFg9awAAAXwAAHPuAAAnoQ==  
-will decode to-  
Hex:	0200F9629400C3669400583D6B0000017C000073EE000027A1  

Reference this table to see how the data is converted into the output:

<img width="663" height="515" alt="image" src="https://github.com/user-attachments/assets/86cc0a97-bbcc-4756-9ad1-c08802d60dd6" />

## **Data Access**
External (public) endpoints this app exposes (replace <HOST> with your FQDN or WAN IP):  
   - Health check     GET http://kyberdyne.ddns.net:5050/health
   - Quick live view  GET http://kyberdyne.ddns.net:5050/live
   - CSV artifact     GET http://kyberdyne.ddns.net:5050/data.csv
   - KML artifact     GET http://kyberdyne.ddns.net:5050/data.kml
   - GeoJSON artifact GET http://kyberdyne.ddns.net:5050/data.geojson


## **References**

[backoffice_var_-provisioning_icd_rev-22.pdf](https://github.com/user-attachments/files/22392468/backoffice_var_-provisioning_icd_rev-22.pdf)  
[BackOffice_Customer_ICD.pdf](https://github.com/user-attachments/files/22392467/BackOffice_Customer_ICD.pdf)  


## **Modules**
process_messages.py — Flask/Gunicorn web app: receives Globalstar posts, decodes, writes CSV/KML/GeoJSON, updates in-memory index, serves /health, /live, /data.*, /devices*. Starts CoT thread only if COT_URL is set (we’ll keep it off during fixes).

persist.py — Creates/maintains artifacts under tracking_data/: kyberdyne_tracking.csv, .kml, .geojson, and device_latest.csv (latest row per device). Exposes ensure_directories(), append_csv(), append_kml(), append_geojson(), and write_latest_row().

device_index.py — In-memory “latest state” per device with sanity checks; supports warm-start from CSV and APIs warm_start(), update(), get_all(), get_one().

payloads.py — Decoder utilities: decode_from_hexstring() for Globalstar fixed layout, decode_b64() for test harness JSON.

cot_out.py — CoT publisher (PyTAK) that reads tracking_data/device_latest.csv and pushes markers to TAK; safe to import (no crash if COT_URL unset).

cot_runner.py (if present) — Simple wrapper that reads env and runs cot_out.py (not strictly needed once saber-cot.service points to cot_out.py directly).

