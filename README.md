# saber_messaging
Python script to receive, parse, store, and promulgate SABER messages received through Globalstar  

Last Update: 17 Sep 25

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
Messages can be viewed in various formats using the following links:  
HTML:  http://kyberdyne.ddns.net:5050/live  
CSV:  http://kyberdyne.ddns.net:5050/data.csv   
KML:  http://kyberdyne.ddns.net:5050/data.kml  
GeoJSON:  http://kyberdyne.ddns.net:5050/data.geojson  

## **References**

[backoffice_var_-provisioning_icd_rev-22.pdf](https://github.com/user-attachments/files/22392468/backoffice_var_-provisioning_icd_rev-22.pdf)  
[BackOffice_Customer_ICD.pdf](https://github.com/user-attachments/files/22392467/BackOffice_Customer_ICD.pdf)  

## **Updating**
Once you update in GitHub, SSH to the Pi and do the following:  
cd ~/globalstar_receiver  
git pull  
sudo systemctl restart globalstar_receiver.service  

