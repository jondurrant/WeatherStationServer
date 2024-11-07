import os
import json
import sys
from datetime import datetime, timedelta
import pandas as pd
import random
from sqlalchemy import create_engine, select, delete, Table, MetaData

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
            "volts": 4.42068, 
            "celcius": 23.8615},
        "aht10": {
            "error": False, 
            "celcius": 19.7811, 
            "max_celcius": 19.8552, 
            "min_celcius": 19.7775, 
            "humidity": 67.9592, 
            "max_humidity": 68.5895, 
            "min_humidity": 67.5763}, 
        "anem": {
            "kmph": 0.0, 
            "max_kmph": 0.0, 
            "min_kmph": 0.0, 
            "gust_kmph": 0.0},  
        "rain": {
            "cumlative_mm": 0.0, 
            "max_mmps": 0.0, 
            "min_mmps": 0.0, 
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
            "celcius": 20.25, 
            "bat_volts": 0.0201416}, 
        "sen0500": {
            "error": False, 
            "celcius": {
                "current": 20.0909, 
                "min": 19.9921, 
                "max": 20.1764}, 
            "humid": {
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
            "degrees": 270.0,
            "max_degrees": 270.0, 
            "min_degrees": 270.0}
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



j["header"]["timestamp"]["year"]=2024
j["header"]["timestamp"]["month"]=10

for day in range(1,31):
    for hour in range(0,23):
        rows=[]
        for minute in range(0,59, 2):
            ts = pd.Timestamp(2024,10,day, hour, minute, 0)
            j["header"]["timestamp"]["hour"]=hour
            j["header"]["timestamp"]["min"]=minute   
            
            j["aht10"]["celcius"] = random.uniform(-10, 25)
            j["aht10"]["min_celcius"] = min(
                random.uniform(-10, 25),
                 j["aht10"]["celcius"])
            j["aht10"]["max_celcius"] = max(
                random.uniform(-10, 25),
                 j["aht10"]["celcius"]) 
                    
            j["aht10"]["humidity"] = random.uniform(10, 90)
            j["aht10"]["min_humidity"] = min(
                random.uniform(10, 90),
                 j["aht10"]["humidity"])
            j["aht10"]["max_humidity"] = max(
                random.uniform(10, 90),
                 j["aht10"]["humidity"]) 
            
            rows.append({
                "Timestamp": ts,
                "DeviceID": "Test1",
                "Type":     j["header"].get("type", "UNKNOWN"),
                "Version":  j["header"].get("version", 0),
                "Payload":  json.dumps(j)
                })
            
        df = pd.DataFrame(rows)
        df.to_sql('DeviceSubmitRaw', con=engine, if_exists='append', index=False)     
            