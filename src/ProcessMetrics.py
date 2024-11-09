#!/usr/bin/env python # 
"""
 TODO
Usage: TODO
"""

from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import sqlalchemy
import os
#from MetricETemp import MetricETemp
from MetricSample import MetricSample
from MetricsChargeCycle import MetricsChargeCycle
from MetricsRainCumlative import MetricsRainCumlative 




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


if __name__ == "__main__":
   
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    #mTemp = MetricETemp(engine)
    #count = 10
    #while count > 0:
    #    print(mTemp.mostRecentTS("Test1"))
    #    count = mTemp.processDevice("Test1")
    #    print("Processed %d"%count)
    
    #mTemp.purge()
    
    #Rain
    rainCum = MetricsRainCumlative("RainCumlative", engine)
    print(rainCum.mostRecentTS("Test1", "rain"))
    count = rainCum.processDevice("Test1", "rain")
    print("Rain Cum Processed %d"%count)
    rainCum.purge()
    
    #ChargeCycle
    chargeCycle = MetricsChargeCycle("ChargeCycle", engine)
    print(chargeCycle.mostRecentTS("Test1", "pico"))
    count = chargeCycle.processDevice("Test1", "pico")
    print("chargeCycle Processed %d"%count)
    chargeCycle.purge()
    
    
    #Temp Sensors
    picoTemp = MetricSample("ITemp", engine)
    print(picoTemp.mostRecentTS("Test1", "pico"))
    count = picoTemp.processDevice("Test1", "pico", picoGetTemp)
    print("picoTemp Processed %d"%count)
    
    rtcTemp = MetricSample("ITemp", engine)
    print(rtcTemp.mostRecentTS("Test1", "rtc"))
    count = picoTemp.processDevice("Test1", "rtc", rtcGetTemp)
    print("rtcTemp Processed %d"%count)
    
    ahtTemp = MetricSample("ETemp", engine)
    print(ahtTemp.mostRecentTS("Test1", "aht10"))
    count = ahtTemp.processDevice("Test1", "aht10", ahtGetTemp)
    print("ahtTemp Processed %d"%count)
    
    senTemp = MetricSample("ETemp", engine)
    print(senTemp.mostRecentTS("Test1", "sen0500"))
    count = senTemp.processDevice("Test1", "sen0500", senGetTemp)
    print("senTemp Processed %d"%count)
    
    senTemp.purge()
    picoTemp.purge()
    
    #Vain
    vain = MetricSample("Vain", engine)
    print(vain.mostRecentTS("Test1", "vain"))
    count = vain.processDevice("Test1", "vain", getVain)
    print("vain Processed %d"%count)
    vain.purge()
    
    #Humidity
    humid = MetricSample("Humidity", engine)
    print(humid.mostRecentTS("Test1", "sen0500"))
    count = humid.processDevice("Test1", "sen0500", senGetHumi)
    print("humid sen0500 Processed %d"%count)
    
    print(humid.mostRecentTS("Test1", "aht10"))
    count = humid.processDevice("Test1", "aht10", ahtGetHumi)
    print("humid aht10 Processed %d"%count)
    
    humid.purge()
    
    
    #Pressure
    pressure = MetricSample("Pressure", engine)
    print(pressure.mostRecentTS("Test1", "sen0500"))
    count = pressure.processDevice("Test1", "sen0500", senGetPressure)
    print("pressure sen0500 Processed %d"%count)
    
    pressure.purge()
    
    
    #Light and UV
    uv = MetricSample("UV", engine)
    print(uv.mostRecentTS("Test1", "sen0500"))
    count = uv.processDevice("Test1", "sen0500", senGetUv)
    print("UV sen0500 Processed %d"%count)
    
    lumi = MetricSample("Lumi", engine)
    print(lumi.mostRecentTS("Test1", "sen0500"))
    count = lumi.processDevice("Test1", "sen0500", senGetLumi)
    print("Lumi sen0500 Processed %d"%count)
    
    uv.purge()
    lumi.purge()
    
    
    #Battery
    bat = MetricSample("Battery", engine)
    print(bat.mostRecentTS("Test1", "pico"))
    count = bat.processDevice("Test1", "pico", picoGetBat)
    print("Bat Pico Processed %d"%count)
    
    print(bat.mostRecentTS("Test1", "rtc"))
    count = bat.processDevice("Test1", "rtc", rtcGetBat)
    print("Bat RTC Processed %d"%count)
    
    bat.purge()
    
    #ChargeVolts
    volts = MetricSample("ChargeV", engine)
    print(volts.mostRecentTS("Test1", "pico"))
    count = volts.processDevice("Test1", "pico", picoGetChargeV)
    print("Charge Pico Processed %d"%count)
    
    volts.purge()
    
    
    
    
    
    
    
    
    