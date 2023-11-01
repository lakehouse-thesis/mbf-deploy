
from pyspark import sql
from delta import *
import os

if "spark" in locals():
    spark.stop()

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages io.delta:delta-core_2.12:2.0.1,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  pyspark-shell'
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

def full_cap_1():
    builder = sql.SparkSession.builder.appName("Delta sub-service high cap")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )\
    .master("yarn")\
    .config("spark.deploy-mode", "client")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.dir", "hdfs://node-master:9000/spark-logs")\
    .config("spark.driver.port", "43900")\
    .config("spark.blockManager.port", "39000")\
    .config("spark.executor.instances", "44")\
    .config("spark.executor.memory", "1474m")\
    .config("spark.driver.cores", "1")\
    .config("spark.driver.memory", "1474m")\
    .config("spark.executor.cores", "1")\
    .config("spark.default.parallelism", "132")\
    .config("spark.default.shuffle.partition", "132")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def full_cap_2(n_exe=4):
    builder = sql.SparkSession.builder.appName("Delta sub-service high cap")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )\
    .master("yarn")\
    .config("spark.deploy-mode", "client")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.dir", "hdfs://node-master:9000/spark-logs")\
    .config("spark.driver.port", "43900")\
    .config("spark.blockManager.port", "39000")\
    .config("spark.executor.instances", str(n_exe))\
    .config("spark.executor.memory", "3g")\
    .config("spark.driver.cores", "1")\
    .config("spark.driver.memory", "3g")\
    .config("spark.executor.cores", "3")\
    .config("spark.default.parallelism", "126")\
    .config("spark.default.shuffle.partition", "126")
    return configure_spark_with_delta_pip(builder).getOrCreate()
def full_cap_3():
    builder = sql.SparkSession.builder.appName("Delta sub-service max cap")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )\
    .master("yarn")\
    .config("spark.deploy-mode", "client")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.dir", "hdfs://node-master:9000/spark-logs")\
    .config("spark.driver.port", "43900")\
    .config("spark.blockManager.port", "39000")\
    .config("spark.executor.instances", "7")\
    .config("spark.executor.memory", "7372m")\
    .config("spark.driver.cores", "3")\
    .config("spark.driver.memory", "7372m")\
    .config("spark.executor.cores", "5")\
    .config("spark.default.parallelism", "105")\
    .config("spark.default.shuffle.partition", "105")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def test_cap():
    builder = sql.SparkSession.builder.appName('Delta sub-service test cap')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .master('yarn')\
        .config('spark.driver.cores', '1')\
        .config('spark.driver.memory', '1g')\
        .config('spark.executor.cores', '1')\
        .config('spark.executor.memory', '1g')\
        .config('spark.executor.instances', '44')\
        .config('spark.default.parallelism', '132')\
        .config('spark.sql.shuffle.partitions', '132')\
        .config('spark.driver.port', '43900')\
        .config('spark.blockManager.port','39000')\
        .config('spark.port.maxRetries', 16)\
        .config('spark.local.dir', 'file:///home/bigdata/spark/logs')\
        .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2')\
        .config('spark.eventLog.enabled', 'true')\
        .config('spark.eventLog.dir', 'hdfs://node-master:9000/spark-logs')\
            
    return configure_spark_with_delta_pip(builder).getOrCreate()

def low_cap():
    builder = sql.SparkSession.builder.appName("Delta sub-service low cap")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )\
    .master("yarn")\
    .config("spark.deploy-mode", "client")\
    .config("spark.eventLog.enabled", "true")\
    .config("spark.eventLog.dir", "hdfs://node-master:9000/spark-logs")\
    .config("spark.driver.port", "43900")\
    .config("spark.blockManager.port", "39000")\
    .config("spark.executor.instances", "1")\
    .config("spark.executor.memory", "1g")\
    .config("spark.driver.cores", "1")\
    .config("spark.driver.memory", "1g")\
    .config("spark.executor.cores", "1")\
    
    return configure_spark_with_delta_pip(builder).getOrCreate()
  # .config('spark.sql.event.truncate.length','40000000000')\
# .config('spark.sql.maxPlanStringLength', "4000000000")\
# .config('spark.kerberos.principal', 'bigdata/bd-node3@FLORUS.LOCAL')\
# .config('spark.kerberos.keytab', '/etc/krb5.spark.keytab')\


