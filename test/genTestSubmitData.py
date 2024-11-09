import os
import json
import sys
from datetime import datetime, timedelta
import pandas as pd
import random
from sqlalchemy import create_engine, select, delete, Table, MetaData

now = pd.Timestamp.utcnow()
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


vbus_sec = 0
vsys_sec = 0
rain_mm = 0

def randDataReset():
    global vbus_sec 
    vbus_sec = 0
    global vsys_sec 
    vsys_sec = 0
    global rain_mm 
    rain_mm = 0


def randData(ts):
    c = random.uniform(-10, 25)
    h = max(random.uniform(-10, 25), c)
    l = min(random.uniform(-10, 35), c) 
    j["aht10"]["celcius"] = {
                "current": c, 
                "max": h, 
                "min": l
                }
                
    c = random.uniform(10, 90)
    h = max(random.uniform(10, 90), c)
    l = min(random.uniform(10, 90), c) 
    j["aht10"]["humidity"] = {
                "current": c, 
                "max": h, 
                "min": l
                } 
    
    c = random.uniform(10, 50)
    h = max(random.uniform(10, 50), c)
    l = min(random.uniform(10, 50), c) 
    j["pico"]["celcius"] = {
                "current": c, 
                "max": h, 
                "min": l
                }
    
    c = random.uniform(2.8, 5.5)
    h = max(random.uniform(2.7, 5.5), c)
    l = min(random.uniform(2.8, 5.5), c) 
    if c < 4.0:
        j["pico"]["source"] = "VSYS"
        global vsys_sec
        vsys_sec = vsys_sec + 120
        j["pico"]["bat_volts"] = {
            "current": c, 
            "max": h, 
            "min": l
            }
        j["pico"]["charge_volts"] = None
    else:
        j["pico"]["source"] = "VBUS"
        global vbus_sec
        vbus_sec = vbus_sec + 120
        j["pico"]["charge_volts"] = {
            "current": c, 
            "max": h, 
            "min": l
            }
        j["pico"]["bat_volts"] = None
    
    j["pico"]["vbus_sec"] = vbus_sec
    j["pico"]["vsys_sec"] = vsys_sec    
    

    c = random.uniform(0.0, 80.0)
    h = max(random.uniform(0.0, 80.0), c)
    l = min(random.uniform(0.0, 80.0), c) 
    j["anem"]["kmph"] = {
                "current": c, 
                "max": h, 
                "min": l
                }
    
    
    c = random.uniform(-1.0, 2.0)
    if c < 0.0:
        c = 0.0
    h = max(random.uniform(0.0, 2.0),c)
    l = min(random.uniform(0.0, 2.0),c)
    global rain_mm
    rain_mm = + rain_mm + c
    j["rain"]= {
            "mmps": {
                "current":  c, 
                "max":      h, 
                "min":      l
                },
            "cumlative_mm": rain_mm, 
            "period_mm": c, 
            "since_sec": 120}
    if c > 0.0:
         j["rain"]["since_sec"] = 0
         
    c = random.uniform(0.0, 40.0)
    h = max(random.uniform(0.0, 40.0), c)
    l = min(random.uniform(0.0, 40.0), c)      
    j["rtc"] = {
            "timestamp": {
                "year":  ts.year, 
                "month": ts.month, 
                "day":   ts.day, 
                "hour":  ts.hour, 
                "min":   ts.minute, 
                "sec": 11}, 
            "celcius": {
                "current": c, 
                "max": h, 
                "min": l
                },      
            "bat_volts": {}}
    c = random.uniform(0.0, 3.0)
    h = max(random.uniform(0.0, 3.0), c)
    l = min(random.uniform(0.0, 3.0), c) 
    j["rtc"]["bat_volts"] = {
                "current": c, 
                "max": h, 
                "min": l
                }

    c = random.uniform(0.0, 359)
    h = max(random.uniform(0.0, 359), c)
    l = min(random.uniform(0.0, 359), c) 
    j["vain"] = {
                "current": c, 
                "max": h, 
                "min": l
                }

    j["sen0500"]["celcius"] =  j["aht10"]["celcius"]
    j["sen0500"]["humidity"] = j["aht10"]["humidity"]

    c = random.uniform(750, 1040)
    h = max(random.uniform(750, 1040), c)
    l = min(random.uniform(750, 1040), c) 
    j["sen0500"]["hPa"] = {
                "current": c, 
                "max": h, 
                "min": l
                }
    
    c = random.uniform(0.0, 100.0)
    h = max(random.uniform(0.0, 100.0), c)
    l = min(random.uniform(0.0, 100.0), c) 
    j["sen0500"]["uv"] = {
                "current": c, 
                "max": h, 
                "min": l
                }
            
    c = random.uniform(0.0, 100.0)
    h = max(random.uniform(0.0, 100.0), c)
    l = min(random.uniform(0.0, 100.0), c) 
    j["sen0500"]["lumi"] = {
                "current": c, 
                "max": h, 
                "min": l
                }



dbHost=os.environ.get("DB_HOST", "localhost")
dbPort=os.environ.get("DB_PORT", "3306")
dbSchema=os.environ.get("DB_SCHEMA", "root")
dbUser=os.environ.get("DB_USER", "root")
dbPasswd=os.environ.get("DB_PASSWD", "root")

# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
print(connectString)
engine = create_engine(connectString)



ts = pd.Timestamp.utcnow() - pd.Timedelta(days=31)
j["header"]["timestamp"]["year"]=ts.year
j["header"]["timestamp"]["month"]=ts.month

#Months worth of data
lastDay = 31
if ts.month == 2:
    lastDay = 28
if ts.month in (9, 4, 6, 11):
    lastDay = 30

#lastDay = 1    
randDataReset()
for day in range(1,lastDay):
    for hour in range(0,23):
        rows=[]
        for minute in range(0,59, 2):
            ts = pd.Timestamp(ts.year,ts.month,day, hour, minute, 0)
            j["header"]["timestamp"]["day"]=day
            j["header"]["timestamp"]["hour"]=hour
            j["header"]["timestamp"]["min"]=minute         
            
            randData(ts)
                   
            rows.append({
                "Timestamp": ts,
                "DeviceID": "Test1",
                "Type":     j["header"].get("type", "UNKNOWN"),
                "Version":  j["header"].get("version", 0),
                "Payload":  json.dumps(j)
                })
            
        df = pd.DataFrame(rows)
        df.to_sql('DeviceSubmitRaw', con=engine, if_exists='append', index=False) 
    randDataReset()   
        
#Back post data on 1st of current month
ts = pd.Timestamp.utcnow()
day = 1
j["header"]["timestamp"]["year"]=ts.year
j["header"]["timestamp"]["month"]=ts.month
j["header"]["timestamp"]["day"]=day
randDataReset()
for hour in range(0,23):
    rows=[]
    for minute in range(0,59, 2):
        ts = pd.Timestamp(ts.year,ts.month,day, hour, minute, 0)
        j["header"]["timestamp"]["day"]=day
        j["header"]["timestamp"]["hour"]=hour
        j["header"]["timestamp"]["min"]=minute   
        
        randData(ts)
        
        if minute >= 30 and minute < 40:
            ts = pd.Timestamp(ts.year,ts.month,day, hour, minute + 10, 30)
        rows.append({
            "Timestamp": ts,
            "DeviceID": "Test1",
            "Type":     j["header"].get("type", "UNKNOWN"),
            "Version":  j["header"].get("version", 0),
            "Payload":  json.dumps(j)
            })
        
    df = pd.DataFrame(rows)
    df.to_sql('DeviceSubmitRaw', con=engine, if_exists='append', index=False)    
        

            