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
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCOREVALUE";
"""
#cursor = conn.cursor()
#cursor.execute(sql)
#data from SAMINDUSTRYNAME
sql2 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SAMINDUSTRY";
"""
#data from SPGASPECT
sql3 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGASPECT";
"""
#data from SPGASSESSMENTTYPE
sql4 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGASSESSMENTTYPE";
"""
#data from SPGSCORETYPE
sql5 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCORETYPE";
"""
#data from SPGSCOREWEIGHT
sql6 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCOREWEIGHT";
"""
#cleaning spgscorevalue dataset
df1 = pd.read_sql(sql1, conn)
df1 = df1.dropna()
df1 = df1.drop_duplicates()
#cleaning samindustry dataset
df2 = pd.read_sql(sql2, conn)
df2 = df2.dropna()
df2 = df2.drop_duplicates()
#cleaning spgaspect dataset
df3 = pd.read_sql(sql3, conn)
df3 = df3.dropna()
df3 = df3.drop_duplicates()
#cleaning spgassessmenttype dataset
df4 = pd.read_sql(sql4, conn)
df4 = df4.dropna()
df4 = df4.drop_duplicates()
#cleaning spgscoretype dataset
df5 = pd.read_sql(sql5, conn)
df5 = df5.dropna()
df5 = df5.drop_duplicates()
#cleaning spgscoreweight dataset
df6 = pd.read_sql(sql6, conn)
df6 = df6.dropna()
df6 = df6.drop_duplicates()

