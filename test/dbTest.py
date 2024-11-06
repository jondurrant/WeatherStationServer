import pandas as pd
from sqlalchemy import create_engine
import os

data = {
    "Timestamp": pd.Timestamp.utcnow(),
    "DeviceID": "Fake1",
    "Type": "Test1",
    "Version": 1.0,
    "Payload": "{\"JSON\": \"Stuff\"}"
    }

df = pd.DataFrame([data])


print(df)

dbHost=os.environ.get("DB_HOST", "localhost")
dbPort=os.environ.get("DB_PORT", "3306")
dbSchema=os.environ.get("DB_SCHEMA", "root")
dbUser=os.environ.get("DB_USER", "root")
dbPasswd=os.environ.get("DB_PASSWD", "root")

# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
print(connectString)
engine = create_engine(connectString)

# Step 3: Convert the Pandas DataFrame to a format for MySQL table insertion
s = df.to_sql('DeviceSubmitRaw', con=engine, if_exists='replace', index=False)

print(s)