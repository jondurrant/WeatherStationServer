#!/usr/bin/env python # 
"""
 Process data from Raw Device Submission into the Analytics Tables
Usage: ProcessMetrics.py
"""
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import sqlalchemy
import os
from MetricSample import MetricSample
from MetricsChargeCycle import MetricsChargeCycle
from MetricsRainCumlative import MetricsRainCumlative 
from pip._vendor.urllib3.util.wait import NoWayToWaitForSocketError




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

def ahtGetHumi(dic):
    return dic.get("aht10", {}).get("humidity", None)

def senGetHumi(dic):
    return dic.get("sen0500", {}).get("humidity", None)

def senGetPressure(dic):
    return dic.get("sen0500", {}).get("hPa", None)

def senGetUv(dic):
    return dic.get("sen0500", {}).get("uv", None)

def senGetLumi(dic):
    return dic.get("sen0500", {}).get("hPa", None)


def rtcGetBat(dic):
    return dic.get("rtc", {}).get("bat_volts", None)

def picoGetBat(dic):
    return dic.get("pico", {}).get("bat_volts", None)

def picoGetChargeV(dic):
    return dic.get("pico", {}).get("charge_volts", None)


def purge(engine):
    #Rain
    rainCum = MetricsRainCumlative("RainCumlative", engine)
    rainCum.purge()
    
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
    

def processQueue(engine):
    
    rainCum = MetricsRainCumlative("RainCumlative", engine)
    chargeCycle = MetricsChargeCycle("ChargeCycle", engine)
    ITemp = MetricSample("ITemp", engine)
    ETemp = MetricSample("ETemp", engine)
    vain = MetricSample("Vain", engine)
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
        count = rainCum.processDevice("Test1", "rain")
        totalCount = totalCount + count
        
        #ChargeCycle
        count = chargeCycle.processDevice("Test1", "pico")
        totalCount = totalCount + count
          
        #Temp Sensors
        count = ITemp.processDevice("Test1", "pico", picoGetTemp)
        totalCount = totalCount + count
        
        count = ITemp.processDevice("Test1", "rtc", rtcGetTemp)
        totalCount = totalCount + count
        
        count = ETemp.processDevice("Test1", "aht10", ahtGetTemp)
        totalCount = totalCount + count
        
        count = ETemp.processDevice("Test1", "sen0500", senGetTemp)
        totalCount = totalCount + count
        
        #Vain
        count = vain.processDevice("Test1", "vain", getVain)
        totalCount = totalCount + count
        
        #Humidity
        count = humid.processDevice("Test1", "sen0500", senGetHumi)
        totalCount = totalCount + count
        count = humid.processDevice("Test1", "aht10", ahtGetHumi)
        totalCount = totalCount + count
        
        #Pressure
        count = pressure.processDevice("Test1", "sen0500", senGetPressure)
        totalCount = totalCount + count    
        
        #Light and UV
        count = uv.processDevice("Test1", "sen0500", senGetUv)
        totalCount = totalCount + count
        
        count = lumi.processDevice("Test1", "sen0500", senGetLumi)
        totalCount = totalCount + count  
        
        #Battery
        count = bat.processDevice("Test1", "pico", picoGetBat)
        totalCount = totalCount + count
        count = bat.processDevice("Test1", "rtc", rtcGetBat)
        totalCount = totalCount + count
        
        #ChargeVolts
        count = volts.processDevice("Test1", "pico", picoGetChargeV)
        totalCount = totalCount + count
        
        print("Processed %d"%totalCount)



if __name__ == "__main__":
   
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    last = pd.Timestamp.utcnow()
    while True:
        processQueue(engine)
        time.sleep(30)
        now = pd.Timestamp.utcnow()
        if (last.day != now.day):
            last = now
            purge(engine)
        
        
    
    
    
    
    
    
    
    
    
    