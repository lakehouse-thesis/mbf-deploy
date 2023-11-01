import sys
sys.path.append("../")
import datetime
from utils import *
spark = full_cap_2(36)
time_range = ["T112022", "T122022", "T012023"]
files = ["DKGOI", "CAT_HUY", "NAPTHE", "DATA", "THOAI"]

file = 'THOAI'
for i in time_range:
    data = spark.read.csv(
        path=f'/data/MBF/upload/VBC_{file}_{i}',
        header=True,)
    data = data.withColumn("ISSUE_DATE", to_date("ISSUE_DATE", 'dd-MMM-yy'))
    data.write.format('csv')\
        .mode('append')\
        .option('header', True)\
        .save(f'/data/MBF/upload/VBC_{file}')

file = 'DKGOI'
for i in time_range:
    data = spark.read.csv(
        path=f'/data/MBF/upload/VBC_{file}_{i}',
        header=True,)
    data = data.withColumn(
        "REG_DATETIME", to_date("REG_DATETIME", date_format))
    data.write.format('csv')\
        .mode('append')\
        .option('header', True)\
        .save(f'/data/MBF/upload/VBC_{file}')

file = 'DATA'
for i in time_range:
    data = spark.read.csv(
        path=f'/data/MBF/upload/VBC_{file}_{i}',
        header=True,)
    if i == 'T012023':
        data = data.withColumnRenamed('ID_HASHED', 'SUB_ID_HASHED')
    if i == 'T112022':
        date_format = 'yyyy-MM-dd'
    else:
        date_format = 'dd-MMM-yy'
    data = data.withColumn("ISSUE_DATE", to_date("ISSUE_DATE", date_format))
    data.write.format('csv')\
        .mode('append')\
        .option('header', True)\
        .save(f'/data/MBF/upload/VBC_{file}')

file = 'NAPTHE'
for i in time_range:
    data = spark.read.csv(
        path=f'/data/MBF/upload/VBC_{file}_{i}',
        header=True,)
    data = data.withColumn("REFILL_DATETIME", to_date(
        "REFILL_DATETIME", 'dd-MMM-yy'))
    data.write.format('csv')\
        .mode('append')\
        .option('header', True)\
        .save(f'/data/MBF/upload/VBC_{file}')