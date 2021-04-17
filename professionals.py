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
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFESSIONAL";
"""

sql2 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROTOPROFUNCTION";
"""

sql3 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFUNCTION";
"""

sql4 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQCOMPENSATION";
"""

sql5 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SP500";
"""

df1 = pd.read_sql(sql1, conn)
df2 = pd.read_sql(sql2, conn)
df3 = pd.read_sql(sql3, conn)
df4 = pd.read_sql(sql4, conn)
df500 = pd.read_sql(sql5, conn)

df5 = pd.merge(df1, df2, how='left', on='PROID')
df6 = pd.merge(df5, df3, how='left', on='PROFUNCTIONID')
df7 = pd.merge(df6, df4, how='left', on='PROID')
df8 = df7[df7['COMPANYID'] == df500['COMPANYID']]

print(df8.head())

#idea of this is to compare males and females (use prefixes: Mr., Ms., and Mrs.,) at the same professional function
#using compensation