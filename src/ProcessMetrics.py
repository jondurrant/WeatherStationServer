#!/usr/bin/env python # 
"""
 Process data from Raw Device Submission into the Analytics Tables
Usage: ProcessMetrics.py
"""
import sys
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import sqlalchemy
import os
from MetricSample import MetricSample
from MetricsChargeCycle import MetricsChargeCycle
from MetricsRainCumlative import MetricsRainCumlative 
from DevicesStatus import DevicesStatus


def picoGetTemp(dic):
    return dic.get("pico", {}).get("celcius", None)

def ahtGetTemp(dic):
    return dic.get("aht10", {}).get("celcius", None)

def senGetTemp(dic):
    return dic.get("sen0500", {}).get("celcius", None)

def rtcGetTemp(dic):
    return dic.get("rtc", {}).get("celcius", None)

def getVain(dic):
    return dic.get("vain", None)

def getAnem(dic):
    return dic.get("anem", {}).get("kmph", None)

def ahtGetHumi(dic):
    return dic.get("aht10", {}).get("humidity", None)

def senGetHumi(dic):
    return dic.get("sen0500", {}).get("humidity", None)

def senGetPressure(dic):
    return dic.get("sen0500", {}).get("hPa", None)

def senGetUv(dic):
    return dic.get("sen0500", {}).get("uv", None)

def senGetLumi(dic):
    return dic.get("sen0500", {}).get("lumi", None)


def rtcGetBat(dic):
    return dic.get("rtc", {}).get("bat_volts", None)

def picoGetBat(dic):
    return dic.get("pico", {}).get("bat_volts", None)

def picoGetChargeV(dic):
    return dic.get("pico", {}).get("charge_volts", None)

def rainGetMMPS(dic):
    return dic.get("rain", {}).get("mmps", None)


def purge(engine):
    #Rain
    rainCum = MetricsRainCumlative("RainCumlative", engine)
    rainCum.purge()
    rainMMPS = MetricSample("Rain", engine)
    rainMMPS.purge()
    
    #ChargeCycle
    chargeCycle = MetricsChargeCycle("ChargeCycle", engine)
    chargeCycle.purge()
    
    #Temp Sensors
    picoTemp = MetricSample("ITemp", engine)   
    senTemp = MetricSample("ETemp", engine)
    senTemp.purge()
    picoTemp.purge()
    
    #Vain
    vain = MetricSample("Vain", engine)
    vain.purge()
    
    #Humidity
    humid = MetricSample("Humidity", engine)
    humid.purge()
    
    #Pressure
    pressure = MetricSample("Pressure", engine)
    pressure.purge()
    
    #Light and UV
    uv = MetricSample("UV", engine)
    lumi = MetricSample("Lumi", engine)
    uv.purge()
    lumi.purge()
      
    #Battery
    bat = MetricSample("Battery", engine)
    bat.purge()
    
    #ChargeVolts
    volts = MetricSample("ChargeV", engine)
    volts.purge()
    

def processQueue(engine, device):
    
    rainCum = MetricsRainCumlative("RainCumlative", engine)
    rainMMPS = MetricSample("Rain", engine)
    chargeCycle = MetricsChargeCycle("ChargeCycle", engine)
    ITemp = MetricSample("ITemp", engine)
    ETemp = MetricSample("ETemp", engine)
    vain = MetricSample("Vain", engine)
    anem = MetricSample("Anem", engine)
    humid = MetricSample("Humidity", engine)
    pressure = MetricSample("Pressure", engine)
    uv = MetricSample("UV", engine)
    lumi = MetricSample("Lumi", engine)
    bat = MetricSample("Battery", engine)
    volts = MetricSample("ChargeV", engine)
    
    totalCount = 1
    while totalCount != 0:
        totalCount = 0
    
        #Rain
        count = rainCum.processDevice(device, "rain")
        totalCount = totalCount + count
        count = rainMMPS.processDevice(device, "rain", rainGetMMPS)
        totalCount = totalCount + count
        
        #ChargeCycle
        count = chargeCycle.processDevice(device, "pico")
        totalCount = totalCount + count
          
        #Temp Sensors
        count = ITemp.processDevice(device, "pico", picoGetTemp)
        totalCount = totalCount + count
        
        count = ITemp.processDevice(device, "rtc", rtcGetTemp)
        totalCount = totalCount + count
        
        count = ETemp.processDevice(device, "aht10", ahtGetTemp)
        totalCount = totalCount + count
        
        count = ETemp.processDevice(device, "sen0500", senGetTemp)
        totalCount = totalCount + count
        
        #Vain
        count = vain.processDevice(device, "vain", getVain)
        totalCount = totalCount + count
        
        #Anem
        count = anem.processDevice(device, "anem", getAnem)
        totalCount = totalCount + count
        
        #Humidity
        count = humid.processDevice(device, "sen0500", senGetHumi)
        totalCount = totalCount + count
        count = humid.processDevice(device, "aht10", ahtGetHumi)
        totalCount = totalCount + count
        
        #Pressure
        count = pressure.processDevice(device, "sen0500", senGetPressure)
        totalCount = totalCount + count    
        
        #Light and UV
        count = uv.processDevice(device, "sen0500", senGetUv)
        totalCount = totalCount + count
        
        count = lumi.processDevice(device, "sen0500", senGetLumi)
        totalCount = totalCount + count  
        
        #Battery
        count = bat.processDevice(device, "pico", picoGetBat)
        totalCount = totalCount + count
        count = bat.processDevice(device, "rtc", rtcGetBat)
        totalCount = totalCount + count
        
        #ChargeVolts
        count = volts.processDevice(device, "pico", picoGetChargeV)
        totalCount = totalCount + count
        
        print("Processed %s %d"%(device, totalCount))



if __name__ == "__main__":
   
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    if len(sys.argv) > 1:
        devices = [sys.argv[1]]
        devicesMgt = None
    else:
        devicesMgt = DevicesStatus(engine)
        devices = devicesMgt.getWeatherStations()
    
    last = pd.Timestamp.utcnow()
    while True:
        for device in devices:
            processQueue(engine, device)
        time.sleep(30)
        now = pd.Timestamp.utcnow()
        if (last.day != now.day):
            last = now
            purge(engine)
        if (devicesMgt != None):
            devices = devicesMgt.getWeatherStations()
            
        
        
    
    
    
    
    
    
    
    
    
    