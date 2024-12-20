import requests
import os
import json
import sys
from datetime import datetime, timedelta



hostname = "localhost"
port = 5000

if len(sys.argv) > 1:
    hostname = sys.argv[1]

if len(sys.argv) == 3:
    port = int(sys.argv[2])

url = ('http://%s:%d/sampleSubmit'%(hostname, port))
newHeaders = {'Content-type': 'application/json', 'Accept': 'text/plain'}


now = datetime.utcnow()
j = {
        "header": {
            "type": "WeatherStation", 
            "version": 1.0, 
            "timestamp": {
                "year": now.year, 
                "month": now.month, 
                "day": now.day, 
                "hour": now.hour, 
                "min": now.minute, 
                "sec": now.second}}, 
        "pico": {
            "id": "Test1", 
            "source": "VBUS", 
            "charge_volts": {
                "current": 4.42068, 
                "min": 4.63096,
                "max": 4.68896
                },
            "bat_volts": {
                "current": 4.42068, 
                "min": 4.63096,
                "max": 4.68896
                },
            "celcius": {
                "current": 23.8615,
                "min": 24.7978, 
                "max": 25.8095
                },
            "vbus_sec": 106.0, 
            "vsys_sec": 30.0
            },
        "aht10": {
            "error": False, 
            "celcius": {
                "current": 19.7811, 
                "max": 19.8552, 
                "min": 19.7775
                }, 
            "humidity": {
                "current": 67.9592, 
                "max": 68.5895, 
                "min": 67.5763
                }
            }, 
        "anem": {
            "kmph": {
                "current": 0.0, 
                "max": 0.0, 
                "min": 0.0
                },
            },  
        "rain": {
            "mmps": {
                "current": 0.0, 
                "max": 0.0, 
                "min": 0.0
                },
            "cumlative_mm": 0.0, 
            "period_mm": 0.0, 
            "since_sec": 121.353}, 
        "rtc": {
            "timestamp": {
                "year": 2024, 
                "month": 11, 
                "day": 3, 
                "hour": 8, 
                "min": 40, 
                "sec": 32}, 
            "celcius": {
                "current": 19.7811, 
                "max": 19.8552, 
                "min": 19.7775
                }, 
            "bat_volts": {
                "current": 0.0201416,
                "max": 1.0,
                "min": 0.0}
            }, 
        "sen0500": {
            "error": False, 
            "celcius": {
                "current": 20.0909, 
                "min": 19.9921, 
                "max": 20.1764}, 
            "humidity": {
                "current": 66.2659, 
                "min": 66.2659, 
                "max": 66.7908}, 
            "hPa": {
                "current": 1026.0, 
                "min": 1025.0, 
                "max": 1026.0}, 
            "uv": {
                "current": 0.0, 
                "min": 0.0, 
                "max": 0.0}, 
            "lumi": {
                "current": 0.0, 
                "min": 0.0, 
                "max": 0.0}}, 
        "vain": {
            "current": 270.0,
            "max": 270.0, 
            "min": 270.0}
        }


x = requests.post(url, json=j, headers=newHeaders)
if (not x.ok):
    print("HTTP ERROR Code %d"%x.status_code)
else:
    t = x.json()
    if (t['status'] == "OK"):
        print("Submitted OK")
    else:
        print("Error submitting")
        
        
t = datetime.utcnow() - timedelta(minutes = 10)
ts = j["header"]["timestamp"]
ts["year"] = t.year
ts["month"] = t.month
ts["day"] = t.day
ts["hour"] = t.hour
ts["min"] = t.minute
ts["sec"] =  t.second
j["pico"]["id"] = "Fake2"

x = requests.post(url, json=j, headers=newHeaders)
if (not x.ok):
    print("HTTP ERROR Code %d"%x.status_code)
else:
    t = x.json()
    if (t['status'] == "OK"):
        print("Submitted OK")
    else:
        print("Error submitting")
