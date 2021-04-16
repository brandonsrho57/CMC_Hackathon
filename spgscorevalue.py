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
#merging datasets 1,2,3,4,5, and 6
df = pd.merge(df12345,df6,how='left',on=['ASPECTID','SAMINDUSTRYID'])
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
# print(mean_esg)
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

#Summary Statistics for ESG SCORES
print(dfesg['SCOREVALUE'].describe())
#Observing the industries who had the highest mean esg score in 2020

dfesg1 = df.loc[(df['ASPECTID']==107)&(df["ASSESSMENTYEAR"]==2020)]
dfesg1.dropna(subset=['ASPECTNAME'])
dfesg1.drop_duplicates()
dfesg1 = dfesg1.groupby('SAMINDUSTRYNAME')['SCOREVALUE'].mean()
dfesg1 = dfesg1.sort_values(ascending=False)
print(dfesg1)
#Tobacco
dfesg2 = df.loc[(df['ASPECTID']==107)&(df["ASSESSMENTYEAR"]==2020)&(df["SAMINDUSTRYNAME"]=='TOB Tobacco')]
dfesg2.dropna(subset=['ASPECTNAME'])
dfesg2.drop_duplicates()
x = dfesg2['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_esg2=dfesg2['SCOREVALUE'].mean()
# print(mean_esg2)
ax.axvline(mean_esg2,color='black',label='Mean ESG SCORE')
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
plt.title('Tobacco Industry ESG Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()
#Restaurants & Leisure Facilities
dfesgrest = df.loc[(df['ASPECTID']==107)&(df["ASSESSMENTYEAR"]==2020)&(df["SAMINDUSTRYNAME"]=='REX Restaurants & Leisure Facilities')]
dfesgrest.dropna(subset=['ASPECTNAME'])
dfesgrest.drop_duplicates()
x = dfesgrest['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_esgrest=dfesgrest['SCOREVALUE'].mean()
# print(mean_esg2)
ax.axvline(mean_esgrest,color='black',label='Mean ESG SCORE')
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
plt.title('Restaurants & Leisure Facilities Industry ESG Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()
#Time Series for Average ESG Mean Scores
dftimeesg = df.loc[(df['ASPECTID']==107)]
dftimeesg.dropna(subset=['ASPECTNAME'])
dftimeesg.drop_duplicates()
dftimeesg = dftimeesg.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
print(dftimeesg)
fig, ax = plt.subplots(1,1)
plt.plot(dftimeesg['ASSESSMENTYEAR'], dftimeesg['SCOREVALUE'],'bo-')
plt.ylim([10, 50])
ax.set(xlabel="Assessment Year",
       ylabel="Average ESG Score Value",
       title="Average ESG Score Value for Each Assessment Year")
for x,y in zip(dftimeesg['ASSESSMENTYEAR'],dftimeesg['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
plt.show()
#Time Series for Tobacco Industry Average ESG Mean Scores
dftimeesgtob = df.loc[(df['ASPECTID']==107)&(df["SAMINDUSTRYNAME"]=='TOB Tobacco')]
dftimeesgtob.dropna(subset=['ASPECTNAME'])
dftimeesgtob.drop_duplicates()
dftimeesgtob = dftimeesgtob.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
# print(dftimeesgtob)
fig, ax = plt.subplots(1,1)
plt.plot(dftimeesgtob['ASSESSMENTYEAR'], dftimeesgtob['SCOREVALUE'],'bo-')
plt.ylim([20, 70])
ax.set(xlabel="Assessment Year",
       ylabel="Average ESG Score Value",
       title="Tobacco Industry Average ESG Score Value for Each Assessment Year")
for x,y in zip(dftimeesgtob['ASSESSMENTYEAR'],dftimeesgtob['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
plt.show()
#Time Series for Restaurants & Leisure Facilities Industry Average ESG Mean Scores
dftimeesgrest = df.loc[(df['ASPECTID']==107)&(df["SAMINDUSTRYNAME"]=='REX Restaurants & Leisure Facilities')]
dftimeesgrest.dropna(subset=['ASPECTNAME'])
dftimeesgrest.drop_duplicates()
dftimeesgrest = dftimeesgrest.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
# print(dftimeesgrest)
fig, ax = plt.subplots(1,1)
plt.plot(dftimeesgrest['ASSESSMENTYEAR'], dftimeesgrest['SCOREVALUE'],'bo-')
plt.ylim([0, 50])
ax.set(xlabel="Assessment Year",
       ylabel="Average ESG Score Value",
       title="Restaurants & Leisure Facilities Industry Average ESG Score Value for Each Assessment Year")
for x,y in zip(dftimeesgrest['ASSESSMENTYEAR'],dftimeesgrest['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
plt.show()
#Counting the amount of companies for each year for Overall ESG Scores
dftimeesgcount = df.loc[(df['ASPECTID']==107)]
dftimeesgcount.dropna(subset=['ASPECTNAME'])
dftimeesgcount.drop_duplicates()
dftimeesgcount=dftimeesgcount.groupby(by='ASSESSMENTYEAR')['INSTITUTIONID'].count().reset_index()
print(dftimeesgcount)
#What does overall social scores look like?
dfsoc = df.loc[(df['ASPECTID']==124)&(df["ASSESSMENTYEAR"]==2020)]
dfsoc.dropna(subset=['ASPECTNAME'])
dfsoc.drop_duplicates()
x = dfsoc['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_soc=dfsoc['SCOREVALUE'].mean()
# print(mean_soc)
ax.axvline(mean_soc,color='black',label='Mean Social SCORE')
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
plt.xlabel('Social Scores', fontsize=15)
plt.ylabel('Frequency', fontsize=15)
plt.title('Overall Social Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()
#Observing the industries who had the highest mean social score in 2020

dfsoc1 = df.loc[(df['ASPECTID']==124)&(df["ASSESSMENTYEAR"]==2020)]
dfsoc1.dropna(subset=['ASPECTNAME'])
dfsoc1.drop_duplicates()
dfsoc1 = dfsoc1.groupby('SAMINDUSTRYNAME')['SCOREVALUE'].mean()
dfsoc1 = dfsoc1.sort_values(ascending=False)
print(dfsoc1)
#Summary Statistics for Social SCORES
print(dfsoc['SCOREVALUE'].describe())
#Time Series for Average Social Mean Scores
dftimesoc = df.loc[(df['ASPECTID']==124)]
dftimesoc.dropna(subset=['ASPECTNAME'])
dftimesoc.drop_duplicates()
dftimesoc = dftimesoc.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
print(dftimesoc)
fig, ax = plt.subplots(1,1)
plt.plot(dftimesoc['ASSESSMENTYEAR'], dftimesoc['SCOREVALUE'],'bo-')
plt.ylim([10, 50])
ax.set(xlabel="Assessment Year",
       ylabel="Average Social Score Value",
       title="Average Social Score Value for Each Assessment Year")
for x,y in zip(dftimesoc['ASSESSMENTYEAR'],dftimesoc['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
plt.show()
#Counting the amount of companies for each year for social
dftimesoccount = df.loc[(df['ASPECTID']==124)]
dftimesoccount.dropna(subset=['ASPECTNAME'])
dftimesoccount.drop_duplicates()
dftimesoccount=dftimesoccount.groupby(by='ASSESSMENTYEAR')['INSTITUTIONID'].count().reset_index()
print(dftimesoccount)
#What does overall environmental scores look like?
dfenv = df.loc[(df['ASPECTID']==108)&(df["ASSESSMENTYEAR"]==2020)]
dfenv.dropna(subset=['ASPECTNAME'])
dfenv.drop_duplicates()
x = dfenv['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_env=dfenv['SCOREVALUE'].mean()
# print(mean_env)
ax.axvline(mean_env,color='black',label='Mean Environmental Score')
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
plt.xlabel('Environmental Scores', fontsize=15)
plt.ylabel('Frequency', fontsize=15)
plt.title('Environmental Social Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()
#Summary Statistics for Environmental SCORES
print(dfenv['SCOREVALUE'].describe())
#Time Series for Average Environmental Mean Scores
dftimeenv = df.loc[(df['ASPECTID']==108)]
dftimeenv.dropna(subset=['ASPECTNAME'])
dftimeenv.drop_duplicates()
dftimeenv = dftimeenv.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
#print(dftimeenv)
fig, ax = plt.subplots(1,1)
plt.plot(dftimeenv['ASSESSMENTYEAR'], dftimeenv['SCOREVALUE'],'bo-')
plt.ylim([10, 50])
ax.set(xlabel="Assessment Year",
       ylabel="Average Environmental Score Value",
       title="Average Environmental Score Value for Each Assessment Year")
for x,y in zip(dftimeenv['ASSESSMENTYEAR'],dftimeenv['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
plt.show()
#What does overall governance scores look like?
dfgov = df.loc[(df['ASPECTID']==114)&(df["ASSESSMENTYEAR"]==2020)]
dfgov.dropna(subset=['ASPECTNAME'])
dfgov.drop_duplicates()
x = dfgov['SCOREVALUE']
plt.style.use('ggplot')
fig, ax=plt.subplots(1,1)
n,bins,patches=ax.hist(x, bins =[0,10,20,30,40,50,60,70,80,90,100], color='orange')
mean_gov=dfgov['SCOREVALUE'].mean()
# print(mean_gov)
ax.axvline(mean_gov,color='black',label='Mean Governance SCORE')
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
plt.xlabel('Governance Scores', fontsize=15)
plt.ylabel('Frequency', fontsize=15)
plt.title('Overall Governance Scores 2020', fontsize=15)
# plot values on top of bars
rects = ax.patches
labels = ["label%d" % i for i in range(len(rects))]
  
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width() / 2, height+0.01, int(height),
            ha='center', va='bottom', fontsize=8)
plt.show()
#Summary Statistics for Governance SCORES
print(dfgov['SCOREVALUE'].describe())
#Time Series for Average Governance Mean Scores
dftimegov = df.loc[(df['ASPECTID']==114)]
dftimegov.dropna(subset=['ASPECTNAME'])
dftimegov.drop_duplicates()
dftimegov = dftimegov.groupby('ASSESSMENTYEAR')['SCOREVALUE'].mean().reset_index()
#print(dftimeenv)
fig, ax = plt.subplots(1,1)
plt.plot(dftimegov['ASSESSMENTYEAR'], dftimegov['SCOREVALUE'],'bo-')
plt.ylim([10, 60])
ax.set(xlabel="Assessment Year",
       ylabel="Average Governance Score Value",
       title="Average Governance Score Value for Each Assessment Year")
for x,y in zip(dftimegov['ASSESSMENTYEAR'],dftimegov['SCOREVALUE']):

    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,12), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
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