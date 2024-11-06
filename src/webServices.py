#!/usr/bin/env python # 
"""
 WebServices HTTP Server
Usage: webServices
"""

from flask import request
from flask import Flask
from datetime import datetime
import json
import socket
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import sqlalchemy
import os


app = Flask(__name__, static_url_path='/static')

@app.route('/', methods=['GET', 'POST'])
def root():
    return "<HTML><BODY><H1>Device Services DrJonEA</H1></BODY?</HTML>"


@app.route('/time', methods=['GET', 'POST'])
def time():
    a = datetime.now()
    t = {
        'year': a.year,
        'month': a.month,
        'day': a.day,
        'hour': a.hour,
        'minute': a.minute,
        'second': a.second
        }

    return json.dumps(t)


@app.route('/sampleSubmit', methods=['POST'])
def jsonSubmit():
    
    if request.is_json:
        sample = request.get_json()
        header = sample.get("header", None)
        pico = sample.get("pico", {})
        if header != None:
            data = {
                "Timestamp": pd.Timestamp.utcnow(),
                "DeviceID": pico.get("id", "UNKNOWN"),
                "Type": header.get("type", "UNKNOWN"),
                "Version": header.get("version", 0),
                "Payload": json.dumps(request.get_json())
                }
            df = pd.DataFrame([data])
            df.to_sql('DeviceSubmitRaw', con=engine, if_exists='append', index=False)
            updateDeviceStatus(sample, request.remote_addr)
        print("Valid JSON: %s"% json.dumps(request.get_json()))
    else:
        print("Invalid JSON");
    
    res = {'status': "OK"}
    return json.dumps(res)


def updateDeviceStatus(sample, addr):
    header = sample.get("header", {})
    pico = sample.get("pico", {})
    now = pd.Timestamp.utcnow()
    ts = header.get("timestamp", {})
    sampleTS = pd.Timestamp(
        ts.get("year", 2024),
        ts.get("month", 1),
        ts.get("day", 1),
        ts.get("hour", 0),
        ts.get("min", 0),
        ts.get("sec", 0),
        tz="UTC"
        )
    d = now - sampleTS
    status = "GOOD"
    message = "NP"
    if (d.value > (60 * 1000000000)):
        status = "WARN"
        message = "Back Posting"
    if (d.value > (24 * 60 * 60 * 1000000000)):
        status = "ERROR"
        message = "Old Data Posting"
        
    data = {
        "Device":   pico.get("id", "UNKNOWN"),
        "Type":     header.get("type", "UNKNOWN"),
        "Version":  header.get("version", 0),
        "LastSeen": now,
        "Status": status,
        "Message": message
        }
    metadata =MetaData()
    DeviceStatusTable = Table(
        'DeviceStatus', 
        metadata,
        sqlalchemy.Column('Device', sqlalchemy.String(length=40), primary_key=True),
        sqlalchemy.Column('Type', sqlalchemy.String(length=40)),
        sqlalchemy.Column('Version', sqlalchemy.Float()),
        sqlalchemy.Column('LastSeen', sqlalchemy.DateTime()),
        sqlalchemy.Column('Status', sqlalchemy.String(length=10)),
        sqlalchemy.Column('Message', sqlalchemy.String(length=256))
        )
    metadata.create_all(engine)
    
    df = pd.DataFrame([data]) 
    
    try: 
        df.to_sql('DeviceStatus', con=engine, if_exists='append', index=False)                 
    except:
        conn = engine.connect()
        mod = DeviceStatusTable.delete().where(DeviceStatusTable.c.Device == data["Device"])
        conn.execute(mod)
        df.to_sql('DeviceStatus', con=conn, if_exists='append', index=False)
        conn.commit()

if __name__ == "__main__":
    app.secret_key = 'DRJONEA'
    app.config['SESSION_TYPE'] = 'filesystem'
    
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    print(f"Hostname: {hostname}")
    print(f"IP Address: {ip_address}")
    
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    app.run(host="0.0.0.0", port="5080")