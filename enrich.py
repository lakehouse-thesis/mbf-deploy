import sys

from utils.schema import Table
sys.path.append("../")
import datetime
from utils import *
spark = full_cap_2(36)
import regex as re
import datetime
def edit_col_name(df,name):
    for i in df.columns[1:]:
        df = df.withColumnRenamed(i, name+re.sub(r'[ ,;{}()\n\t=#-.|]','',re.sub(r'{name}'.format(name='reg'),'',i)))
    return df

# ĐĂNG KÝ GÓI
goicuoc = spark.read.csv('file:///home/bigdata/setup/GoiCuocMBF.csv', header=True,inferSchema=True).drop_duplicates()
dkgoi=spark.read.format('delta').load(path = '/data/MBF/silver/VBC_DKGOI', header=True, inferSchema=True)
dkgoi = dkgoi.join(goicuoc,dkgoi.CODE==goicuoc.TEN_GOI)\
        .withColumnRenamed('SUB_ID_HASHED','SUB_ID')\
        .withColumn('END_DATETIME',expr('date_add(to_date(REG_DATETIME,"dd-MMM-yy"), cast(NGAY_SUDUNG as int))'))\
        .withColumn('REMAIN', (datediff('END_DATETIME',lit( datetime.datetime(2023, 1, 1))).cast(IntegerType())>0).cast(IntegerType()))\
        .withColumn('REMAIN',when(isnull('REMAIN'),0).otherwise(col('REMAIN').cast(IntegerType())))\
        .withColumn('REG_DATETIME',date_trunc('mon', to_date(col('REG_DATETIME'),'dd-MMM-yy')))\
        .withColumn('END_DATETIME',date_trunc('mon', to_date(col('END_DATETIME'),'dd-MMM-yy')))\
        .filter(expr("REG_DATETIME < to_date('01-Jan-23','dd-MMM-yy')"))\



reg_subid = dkgoi.groupBy('SUB_ID')

reg_date = reg_subid.pivot('REG_DATETIME').sum('CHARGE_PRICE').fillna(0)
reg_date = reg_date.withColumn('log_nov',log10(col(reg_date.columns[1])+1))\
    .withColumn('log_dec',log10(col(reg_date.columns[2])+1))\
    # .select('SUB_ID','nov','dec')
drop_values = ['API_GW_SMS','C2C','DATASPONSOR','INSP','KNDL','M90','MOBIFONE_MONEY','MOBIFONE_PAY','TMDT','TMDT_SMS','BILLING', 'BILLING_SMS', 'CALLOUTCT2', 'CCSPS', 'CHATBOT_SMS', 'CNTT', 'M090', 'MBFCBS', 'MBIZZ', 'MBIZZ_CONF', 'MYMOBI', 'MYMOBI_SMS', 'SELFCARE', 'SELFCARE_FO', 'SRA_SMS', 'TEST', 'USSD090', 'VAS', 'VASS', 'VAS_ONLINE']
reg_code = dkgoi.filter(~ dkgoi.REG_CODE.isin(drop_values)).groupBy('SUB_ID').pivot('REG_CODE').count()

reg_remain_charge = reg_subid.pivot('REMAIN').sum('CHARGE_PRICE')
reg_remain_charge = reg_remain_charge.withColumn('reg_deactive',log10(col('0')+1))\
    .withColumn('reg_active',log10(col('1')+1))\
    
reg_end_date = reg_subid.pivot('CHU_KY').sum('REMAIN')

reg_charge_price = reg_subid.pivot('CHARGE_PRICE',[0]).count().withColumnRenamed('0','charge0')
reg_code_type=reg_subid.pivot('PSEU_LOAI_GOI').count()
reg=reg_date.join(reg_code,on='SUB_ID', how='left')\
    .join(reg_end_date,on='SUB_ID', how='left')\
    .join(reg_remain_charge,on='SUB_ID', how='left')\
    .join(reg_charge_price,on='SUB_ID', how='left')\
        .join(reg_code_type,on='SUB_ID', how='left')\
        .orderBy('SUB_ID')
reg.persist(StorageLevel.DISK_ONLY)
reg = edit_col_name(reg,'reg')
reg.fillna(0).write.format('delta').mode('overwrite').save('/data/MBF/unregistered/encode/last_date/reg')



# NẠP THẺ
napthe=spark.read.format('delta').load(path = '/data/MBF/silver/VBC_NAPTHE', header=True, inferSchema=True)
napthe = napthe\
            .withColumn('REFILL_DATE',date_trunc('mon', to_date(napthe.REFILL_DATETIME,'yyyy-MM-dd')))\
        .withColumnRenamed('SUB_ID_HASHED','SUB_ID')\
            .filter(expr("REFILL_DATE < to_date('01-Jan-23','dd-MMM-yy')"))\

refill_subid = napthe.coalesce(105).groupBy('SUB_ID')

refill_date = refill_subid.pivot('REFILL_DATE').sum('REFILL_AMOUNT').fillna(0)
refill_date = refill_date.withColumn('log_nov',log10(col(reg_date.columns[1])+1))\
    .withColumn('log_dec',log10(col(reg_date.columns[2])+1))
last_refill = refill_subid.agg(max('REFILL_DATETIME').alias('last_refill'))
last_refill = last_refill.withColumn('last_refill',datediff(lit( datetime.datetime(2023, 1, 1)),'last_refill').cast(IntegerType()))

drop_values = ['*KXD', 'TT1', 'TT4', 'TT5', 'TT6', 'TT7']
refill_cencode = napthe.filter(~ napthe.CEN_CODE.isin(drop_values)).groupBy('SUB_ID').pivot('CEN_CODE').count()
refill_saleinfo = napthe.filter(napthe.SALE_INFO!=0).groupBy('SUB_ID').pivot('SALE_INFO').count()

refill =refill_date.join(refill_cencode,on='SUB_ID')\
    .join(last_refill,on='SUB_ID')\
    .join(refill_saleinfo,on='SUB_ID')\
        .orderBy('SUB_ID')

refill = edit_col_name(refill,'refill')
refill.fillna(0).write.format('delta').mode('overwrite').save('/data/MBF/unregistered/encode/last_date/refill')



# THOẠI
thoai=spark.read.format('delta').load(path = '/data/MBF/silver/VBC_THOAI', header=True, inferSchema=True)
thoai = thoai\
            .withColumn('ISSUE_MONTH',date_trunc('mon', to_date(thoai.ISSUE_DATE,'yyyy-MM-dd')))\
            .filter(expr("ISSUE_MONTH < to_date('01-Jan-23','dd-MMM-yy')"))\

call_subid = thoai.coalesce(105).groupBy('SUB_ID')

call_date = call_subid.pivot('ISSUE_MONTH').agg({'CREDIT':'sum'}).fillna(0)
call_date = call_date.withColumn('log_nov',log10(col(call_date.columns[1])+1))\
    .withColumn('log_dec',log10(col(call_date.columns[2])+1))\
    
last_call = call_subid.agg(max(to_date(thoai.ISSUE_DATE,'yyyy-MM-dd')).alias('last'))
last_call = last_call.withColumn('last',datediff(lit( datetime.datetime(2023, 1, 1)),'last').cast(IntegerType()))
  

call_totalcredit = call_subid.pivot('TOTAL_CREDIT',[0]).count()
call_province = call_subid.pivot('PROVINCE_ID').count()
call_province = edit_col_name(call_province,'provinceid')
call_subitemid = call_subid.pivot('SUB_ITEM_ID').count()
call_subitemid = edit_col_name(call_subitemid,'subitemid')
call_bustype = call_subid.pivot('BUS_TYPE').count()
call_bustype=edit_col_name(call_bustype,'bustype')
call_subtype = call_subid.pivot('SUB_TYPE').count()
call_subtype=edit_col_name(call_subtype,'subtype')
call_profile = call_subid.pivot('PROFILE').count()
call_profile=edit_col_name(call_profile,'profile')
call_name = call_subid.pivot('NAME').count()
call_name=edit_col_name(call_name,'name')
call_itemid = call_subid.pivot('ITEM_ID').count()
call_itemid = edit_col_name(call_itemid,'itemid')
call_tlpc_ttvas = call_subid.pivot('TLPC_TTVAS').count()
call =call_date\
        .join(last_call,on='SUB_ID')\
        .join(call_totalcredit,on='SUB_ID')\
        .join(call_province,on='SUB_ID')\
        .join(call_subitemid,on='SUB_ID')\
        .join(call_bustype,on='SUB_ID')\
        .join(call_subtype,on='SUB_ID')\
        .join(call_profile,on='SUB_ID')\
        .join(call_name,on='SUB_ID')\
        .join(call_itemid,on='SUB_ID')\
        .join(call_tlpc_ttvas,on='SUB_ID')\
    .orderBy('SUB_ID')


call.persist(StorageLevel.DISK_ONLY)
call = edit_col_name(call,'')
call = call.drop('provinceidnull', 'provinceid1', 'provinceid31778', 'provinceid31790', 'provinceid31792', 'provinceid31802','subitemidnull', 'subitemid09090', 'subitemid15027', 'subitemid106x', 'subitemid108/801', 'subitemid111', 'subitemid15105', 'subitemid15129', 'subitemid15130', 'subitemid15131', 'subitemid15132', 'subitemid155026', 'subitemid155036', 'subitemid155422',  'subitemid155546', 'subitemid155651', 'subitemid1570', 'subitemid1800CMC', 'subitemid1800FPT', 'subitemid1800GTEL', 'subitemid1800HTC', 'subitemid1800ITEL', 'subitemid1800SPT','subitemid1800VNP', 'subitemid1800VTL', 'subitemid1900xxxx', 'subitemid1900CMC', 'subitemid1900FPT', 'subitemid1900GTEL',   'subitemid1900HTC', 'subitemid1900ITEL', 'subitemid1900SPT', 'subitemid1900VNP','subitemid1900VTC', 'subitemid1900VTL', 'subitemid9129', 'subitemidBK', 'subitemidBTB', 'subitemidC19', 'subitemidC29', 'subitemidC3', 'subitemidC39', 'subitemidC49', 'subitemidC9', 'subitemidCK100', 'subitemidCK30', 'subitemidCK50', 'subitemidCK70', 'subitemidCS68T', 'subitemidDU1', 'subitemidHN', 'subitemidK100', 'subitemidK150', 'subitemidK250', 'subitemidK350', 'subitemidK50', 'subitemidK9', 'subitemidK90', 'subitemidKB',  'subitemidMFWD', 'subitemidQTTK15', 'subitemidT29', 'subitemidTAM', 'subitemidTN50', 'subitemidTNX', 'subitemidTQT19', 'subitemidTQT199', 'subitemidTQT299', 'subitemidTQT49', 'subitemidTQT49KGH', 'subitemidTQT9', 'subitemidTQT99', 'subitemidTongdaiktxh', 'subitemidV100N', 'subitemidV120N', 'subitemidV150N', 'subitemidV20N', 'subitemidV30N', 'subitemidV50N', 'subitemidV70N', 'subitemidY10', 'subitemidY5', 'subitemidwitalk', 'bustypenull', 'bustypeAM', 'bustypeAM2', 'bustypeCP', 'bustypeDNN','bustypeFOR', 'bustypeGOV', 'bustypeHKD', 'bustypeHTX', 'bustypeJVC', 'bustypeLM1', 'bustypeLSQ', 'bustypePTA', 'bustypePTE', 'bustypeREP', 'bustypeTR1', 'bustypeTR2', 'bustypeVI1', 'bustypeVI2', 'bustypeVI3', 'bustypeVI4', 'bustypeVI5', 'bustypeVIE', 'bustypeVMS', 'bustypeZBZ', 'subtypenull', 'subtypeASZ', 'subtypeB4G', 'subtypeBKS', 'subtypeBTR', 'subtypeCMQ', 'subtypeCP', 'subtypeDCF', 'subtypeDLM', 'subtypeDNN', 'subtypeET1', 'subtypeETK', 'subtypeEZ', 'subtypeFC', 'subtypeFCE', 'subtypeFCZ', 'subtypeHDE', 'subtypeHTV', 'subtypeLM1', 'subtypeLME',  'subtypeMGL', 'subtypeMSY', 'subtypeMY', 'subtypeMZ', 'subtypePOL', 'subtypePTE', 'subtypePZ', 'subtypeQDV', 'subtypeQT4', 'subtypeQTP', 'subtypeRS', 'subtypeRZ', 'subtypeS30', 'subtypeSEA', 'subtypeSP',  'subtypeVI2', 'subtypeVIE', 'subtypeW2G', 'subtypeWT', 'subtypeXC', 'subtypeXU', 'subtypeZDT', 'subtypeZHN', 'subtypeZMT', 'subtypeZPP','profileBKS', 'profileDCF', 'profileDHMT', 'profileFCDN', 'profileFCT2', 'profileFCZ', 'profileMCP', 'profilePOLU','profileRZT2', 'profileSBK', 'profileSVT2', 'profileTIT', 'profileTTD2',  'profileTTGTEL', 'profileW2G', 'profileWT2', 'profileYT2', 'profileZHN', 'profileZMT',  'nameG�itho?i', 'nameG�itho?iqu?ct?', 'nameTho?i??nkhiRoamingQu?ct?',  'nameTho?i?ikhiRoamingQu?ct?', 'nameTho?ic???nhViettel', 'nameTho?ic???nhkh�c',   'nameTho?idi??ngGTel', 'nameTho?idi??ngITelecom',   'nameTho?iqu?ct?', 'itemid3', 'itemid4', 'itemid8', 'itemid12', 'itemid13','null', '00')
call = edit_col_name(call,'call').fillna(0)
call.write.format('delta').mode('overwrite').save('/data/MBF/unregistered/encode/last_date/call')# .columns

# DATA
data4g=spark.read.format('delta').load(path = '/data/MBF/silver/VBC_DATA', header=True, inferSchema=True)
data4g = data4g\
            .withColumn('last',datediff(lit( datetime.datetime(2023, 1, 1)),to_date(data4g.ISSUE_DATE,'yyyy-MM-dd')).cast(IntegerType()))\
            .withColumn('ISSUE_DATE',date_trunc('mon', to_date(data4g.ISSUE_DATE,'yyyy-MM-dd')))\
            .withColumnRenamed('SUB_ID_HASHED','SUB_ID')\
            .filter(expr("ISSUE_DATE < to_date('01-Jan-23','dd-MMM-yy')"))\
 
usage_subid = data4g.coalesce(105).groupBy('SUB_ID')
usage_date = usage_subid.pivot('ISSUE_DATE').agg({'DOANHTHU':'sum'}).fillna(0)
usage_date = usage_date.withColumn('log_nov',log10(col(usage_date.columns[1])+1))\
    .withColumn('log_dec',log10(col(usage_date.columns[2])+1))\

last_usage = usage_subid.agg(max('last').alias('last'))

usage_amount = usage_subid.pivot('DATA',[0]).count().withColumnRenamed('0','0MB')
usage_revenue = usage_subid.pivot('DOANHTHU',[0]).count().withColumnRenamed('0','charge0')
usage4g =usage_date\
    .join(last_usage,on='SUB_ID')\
    .join(usage_amount,on='SUB_ID')\
        .join(usage_revenue,on='SUB_ID')\
    .orderBy('SUB_ID')
usage4g.persist(StorageLevel.DISK_ONLY)
usage4g=edit_col_name(usage4g,'data')
usage4g.fillna(0).write.format('delta').mode('overwrite').save('/data/MBF/unregistered/encode/last_date/data4g')

# All features
reg=spark.read.format("delta").load('/data/MBF/unregistered/encode/last_date/reg')
refill=spark.read.format("delta").load('/data/MBF/unregistered/encode/last_date/refill')
call=spark.read.format("delta").load('/data/MBF/unregistered/encode/last_date/call')
usage4g=spark.read.format("delta").load('/data/MBF/unregistered/encode/last_date/data4g')


all = reg.join(refill,on='SUB_ID',how='full').join(call,'SUB_ID',how='full').join(usage4g,'SUB_ID',how='full')
len(all.columns)

all = all.withColumn('last_active',when(expr('calllast>datalast'),col('calllast')).when(expr('calllast>datalast'),col('datalast')).otherwise(lit(60)))\
.withColumn('''callnameTho?idi??ngViettelVina''',col('''callnameTho?idi??ngVinaphone''') + col('''callnameTho?idi??ngViettel'''))\
.drop('''callnameTho?idi??ngVinaphone''','''callnameTho?idi??ngViettel''')
all = spark.read.format('delta').load('/data/MBF/unregistered/encode/last_date/full_join')

stream_id = all.getQueryId()

def mongo(table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('cache')
    return db[table_name]
db = delta()
table_metadata = Table('MBF', 'silver', 'VBC_ENRICH', stream_id, "", fields=all.getSchema())
db.insert_one(vars(table_metadata))

    


reg.unpersist()
refill.unpersist()
call.unpersist()
usage4g.unpersist()
if "spark" in locals():
    spark.stop()