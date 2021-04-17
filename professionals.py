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

sql5 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SP500";
"""
df500 = pd.read_sql(sql5, conn)

sql1 = """
SELECT companyId, proId, personId FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFESSIONAL";
"""

sql2 = """
SELECT proId, profunctionId FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROTOPROFUNCTION";
"""

sql3 = """
SELECT profunctionId FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFUNCTION";
"""

sql4 = """
SELECT proId, compensationValue FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQCOMPENSATION";
"""

sql6 = """
SELECT personId, prefix FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPERSON";
"""

df1 = pd.read_sql(sql1, conn)
#print(df1.head())
df2 = pd.read_sql(sql2, conn)
df3 = pd.read_sql(sql3, conn)
df4 = pd.read_sql(sql4, conn)

dfperson = pd.read_sql(sql6, conn)

df5 = pd.merge(df1, df2, how='left', on='PROID')
df6 = pd.merge(df5, df3, how='left', on='PROFUNCTIONID')
df7 = pd.merge(df6, df4, how='left', on='PROID')
df9 = pd.merge(df7, dfperson, how='left', on='PERSONID')
df8 = df9[df9['COMPANYID'].isin(df500['COMPANYID'])]

df8.dropna(subset=['COMPENSATIONVALUE'])

df8 = df8.groupby(["PROFUNCTIONID", "PREFIX"])["COMPENSATIONVALUE"].mean()
print(df8)


#idea of this is to compare males and females (using prefixes: Mr., Ms., and Mrs.,) at the same professional function
#using compensation