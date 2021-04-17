import snowflake.connector as sf
import pandas as pd
import matplotlib.pyplot as plt
from config import config
import numpy as np

# Connection String
conn = sf.connect(
    user=config.username,
    password=config.password,
    account=config.account
)

def test_connection(connect, query):
    cursor = connect.cursor()
    cursor.execute(query)
    cursor.close()

sql1 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFESSIONALS";
"""

sql2 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQKEYDEVTOOBJECTTOEVENTTYPE";
"""

sql3 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQKEYDEVEVENTTYPE";
"""

#cursor = conn.cursor()
#cursor.execute(sql)
#data from SAMINDUSTRYNAME

#cleaning spgscorevalue dataset
df1 = pd.read_sql(sql1, conn)


#df2 = pd.read_sql(sql2, conn)

#df3 = pd.read_sql(sql3, conn)

#df4 = pd.merge(df1, df2, how='left', on="KEYDEVID")

#df5 = pd.merge(df4, df3, how='left', on="KEYDEVEVENTTYPEID")

#df6 = df5[df5["KEYEVENTTYPEID"] = 149]



