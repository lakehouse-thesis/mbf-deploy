
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter

def write_data(stream: DataStreamWriter, table_name):
    def write_to_both(udf, batchID):
        udf.persist()
        write_mongo(udf, table_name)
        write_hdfs(udf, table_name)
        udf.unpersist()
    stream.writeStream.foreachBatch(write_to_both).start()

def write_hdfs(stream: SparkSession, table_name, mode="append"):
    stream.write.format("csv").option(
        "path", f"/data/MBF/unregistered/{table_name}"
    ).option('header','true')\
    .option("checkpointLocation", f"/checkpoint/MBF/unregistered/{table_name}").mode(
        mode
    ).save()



def write_mongo(stream: SparkSession, table_name, mode="append"):
    stream.write.format("mongodb").option("header", "True").option(
        "checkpointLocation", f"/checkpoint/MBF/{table_name}"
    ).option(
        "spark.mongodb.connection.uri",
        "mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin",
    ).option(
        "spark.mongodb.database", "cache"
    ).option(
        "spark.mongodb.collection", f'{table_name}'
    ).mode(
        mode
    ).save()

def write_pandas_to_mongo(df, table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('cache')
    table = db[table_name]
    # table.delete_many({})
    table.insert_many(df.to_dict('records'))      



def mongo(table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('cache')
    return db[table_name]

def delete_mongo(table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('cache')
    return db.drop_collection(table_name).keys()

def delta(table_name):
    from pymongo import MongoClient
    client = MongoClient("mongodb://delta:floruS.2022@mongo-db1:27017,mongo-db2:27017,mongo-db3:27017/admin")
    db = client.get_database('delta_tables')
    return db[table_name]
