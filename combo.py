import sys
sys.path.append("../")
import datetime
from utils import *
from utils.schema import *
spark = full_cap_2(1)

df_thoai = spark.read.format('delta')\
    .load(f'/data/MBF/bronze/VBC_THOAI',header=True, inferSchema = True).head(5)
df_thoai = df_thoai.withColumn('month', date_trunc('mm','ISSUE_DATE'))\
    .withColumn('CREDIT',expr('cast(CREDIT as int)'))\
    .groupBy('SUB_ID').pivot('month').agg(sum('CREDIT'))

df_data = spark.read.format('delta')\
    .load(f'/data/MBF/bronze/VBC_DATA',header=True, inferSchema = True).head(5)
df_data = df_data.withColumn('month', date_trunc('mm','ISSUE_DATE'))\
    .withColumn('CREDIT',expr('cast(DOANHTHU as int)'))\
    .groupBy('SUB_ID').pivot('month').agg(sum('CREDIT'))

df_combo = df_thoai.alias('A').join(df_data.alias('B'), ['SUB_ID'])\
    .withColumn('call_nov',expr('A.select(2022-11-01 00:00:00).fillna(0)'))\
    .withColumn('call_dec',expr('A.select(2022-12-01 00:00:00).fillna(0)'))\
    .withColumn('call_jan',expr('A.select(2023-01-01 00:00:00).fillna(0)'))\
    .withColumn('data_nov',expr('B.select(2022-11-01 00:00:00).fillna(0)'))\
    .withColumn('data_dec',expr('B.select(2022-12-01 00:00:00).fillna(0)'))\
    .withColumn('data_jan',expr('B.select(2023-01-01 00:00:00).fillna(0)'))\
    .withColumn('Nov',expr('call_nov+data_nov'))\
    .withColumn('Dec',expr('call_dec+data_dec'))\
    .withColumn('Jan',expr('call_jan+data_jan'))\


df_combo.write.format('delta')\
        .mode('append')\
        .option('header', True)\
        .save(f'/data/MBF/silver/VBC_COMBO')
stream_id = df_combo.getQueryId()

def mongo(table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('cache')
    return db[table_name]
db = delta()
table_metadata = Table('MBF', 'silver', 'VBC_COMBO', stream_id, "", fields=df_combo.getSchema())
db.insert_one(vars(table_metadata))

    
