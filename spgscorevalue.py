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

# try:
#     sql = """
#     SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCOREVALUE"
#     LIMIT 5;
#     """
#     test_connection(conn, sql)
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     for line in cursor:
#         print(line)
# except Exception as e:
#     print(e)

#data from SPGSCOREVALUE
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
#print(df6.head())
#merging datasets 1 and 2
df12 = pd.merge(df1,df2,how='left', on='SAMINDUSTRYID')
#print(df12)
#merging datasets 1,2, and 3
df123 = pd.merge(df12,df3,how='left', on='ASPECTID')
#df123 = df[['ASPECTID','ASPECTNAME','SAMINDUSTRYNAME','SAMINDUSTRYID','SCOREVALUE']]
#merging datasets 1,2,3, and 4
df1234 = pd.merge(df123,df4,how='left',on='ASSESSMENTTYPEID')
#df1234 = df1234[['ASSESSMENTTYPEID','ASSESSMENTTYPENAME','SAMINDUSTRYNAME','SAMINDUSTRYID','SCOREVALUE']]
#merging datasets 1,2,3,4, and 5
df12345 = pd.merge(df1234,df5,how='left',on='SCORETYPEID')
#df12345 = df12345[['SCORETYPEID','SCORETYPENAME','SAMINDUSTRYNAME','SAMINDUSTRYID','SCOREVALUE']]
#merging datasets 1,2,3,4, and 5
# df = pd.merge(df12345,df6,how='left',on=['ASPECTID','SAMINDUSTRYID'])
# df = df[['ASPECTID','ASPECTNAME']]
# df = df.loc[(df['ASPECTID']==107) | (df['ASPECTID']==108) | (df['ASPECTID']==114) | (df['ASPECTID']==124)]
# df = df.sort_values(by='ASPECTID').drop_duplicates()
# print(df)
# df = df[['SAMINDUSTRYID','ASPECTID','SAMINDUSTRYNAME','SAMINDUSTRYID','FROMDATE','TODATE']]
# print(df.head())
# only filtering out REA Real Estate
dfRE = df[df["SAMINDUSTRYNAME"]=='REA Real Estate']
dfRE = dfRE[['SCOREID','INSTITUTIONID','ASPECTID','SAMINDUSTRYNAME','SCOREVALUE']]
dfRE = dfRE.sort_values(by='SCOREVALUE', ascending=False).head(3)
print(dfRE)
#Question 1 Total Overview of ESG SCORES by making Histogram of all companies ESG Scores
dfesg = df.loc[(df['ASPECTID']==107)&(df["ASSESSMENTYEAR"]==2020)]
dfesg.dropna(subset=['ASPECTNAME'])
dfesg.drop_duplicates()
x = dfesg['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_esg=dfesg['SCOREVALUE'].mean()
#print(mean_esg)
ax.axvline(mean_esg,color='black',label='Mean ESG SCORE')
ax.legend(loc=0)
patches[0].set_fc("red")
patches[1].set_fc("yellow")
patches[2].set_fc("green")
patches[3].set_fc("blue")
patches[4].set_fc("aqua")
patches[5].set_fc("indigo")
patches[6].set_fc("lavendar")
patches[7].set_fc("salmon")
patches[8].set_fc("grey")
plt.xticks(bins)
plt.xlabel('ESG Scores', fontsize=15)
plt.ylabel('Frequency', fontsize=15)
plt.title('Overall ESG Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()

#filtering out Assessment Year = 2020 and grouping by industry and making it descending order
yr_2020 = df.loc[df["ASSESSMENTYEAR"]==2020]
yr_2020 = yr_2020.sort_values(by='SCOREVALUE', ascending=False).groupby(by='SAMINDUSTRYID').head(3)
print(yr_2020)
# for line in df:
#     print(line)
# t = df.iloc[1:3]
# plt.plot(t["PRICEDATE"],t["PRICECLOSE"])
# plt.show()