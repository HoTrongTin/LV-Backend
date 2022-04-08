from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from sparkSetup import spark
from delta.tables import *
from mongodb import CacheQuery
import json

import configparser

#config
config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
amazonS3param = config_obj["amazonS3"]

#streaming HDFS To Bronze
def streamingHDFSToBronze(tableName, schema):
    streamingSchema = StructType()
    for col in schema:
        if len(col) == 2:
            streamingSchema.add(col[0], col[1])
        else: streamingSchema.add(col[0], col[1], col[2])

    dfStreaming = spark.readStream.option("sep", ",").option("header", "true").schema(streamingSchema).csv("/streaming/" + tableName).withColumn('Date_Time', current_timestamp())
    dfStreaming.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/" + tableName).start("/medical/bronze/" + tableName)
    print('Stream name: ' + dfStreaming.name)

#streaming S3 To Bronze
def streamingS3ToBronze(tableName, schema):
    streamingSchema = StructType()
    for col in schema:
        if len(col) == 2:
            streamingSchema.add(col[0], col[1])
        else: streamingSchema.add(col[0], col[1], col[2])

    dfStreaming = spark.readStream.option("sep", ",").option("header", "true").schema(streamingSchema).csv(amazonS3param['s3aURL'] + "/medical/" + tableName).withColumn('Date_Time', current_timestamp())
    dfStreaming.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/medical/checkpoint/bronze/" + tableName).start("/medical/bronze/" + tableName)

#streaming Bronze To Gold With Merge Method
def streamingBronzeToGoldMergeMethod(tableName, schema, mergeOn, partitionedBy = []):
    def upsertToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        mergeOnparser = ''
        for oncol in mergeOn:
            mergeOnparser += 'silver_' + tableName + '.' + oncol + ' = s.' + oncol + ' AND '

        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/medical/silver/""" + tableName + """` silver_""" + tableName + """
            USING updates s
            ON """ + mergeOnparser[:-5] + """
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    setColumns = ', '.join([' '.join(x for x in col if isinstance(x, str)) for col in schema])
    setPartitionedBy = ', '.join(partitionedBy)

    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/' + tableName)):
        spark.sql("CREATE TABLE bronze_" + tableName + " (" + setColumns + ", Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/" + tableName + "'")

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/' + tableName)):
        spark.sql("CREATE TABLE silver_" + tableName + " (" + setColumns + ", Date_Time timestamp) USING DELTA LOCATION '/medical/silver/" + tableName + "'" + (" PARTITIONED BY (" + setPartitionedBy + ")" if partitionedBy != [] else ''))

    dfStreaming = spark.readStream.format("delta").load("/medical/bronze/" + tableName)
    dfStreaming.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/" + tableName).outputMode("update").foreachBatch(upsertToDelta).start()

#streaming Bronze To Gold With Append Method
def streamingBronzeToGoldAppendMethod(tableName, schema, partitionedBy = []):
    def appendToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("batchData")

        setFields = ', '.join([field[0] for field in schema])

        microBatchOutputDF._jdf.sparkSession().sql("""
            INSERT INTO delta.`/medical/silver/""" + tableName + """`
            (""" + setFields + """, Date_Time)
            SELECT """ + setFields + """, Date_Time
            FROM batchData
        """)

    setColumns = ', '.join([' '.join(x for x in col if isinstance(x, str)) for col in schema])
    setPartitionedBy = ', '.join(partitionedBy)

    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/medical/bronze/' + tableName)):
        spark.sql("CREATE TABLE bronze_" + tableName + " (" + setColumns + ", Date_Time timestamp) USING DELTA LOCATION '/medical/bronze/" + tableName + "'")

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/medical/silver/' + tableName)):
        spark.sql("CREATE TABLE silver_" + tableName + " (" + setColumns + ", Date_Time timestamp) USING DELTA LOCATION '/medical/silver/" + tableName + "'" + (" PARTITIONED BY (" + setPartitionedBy + ")" if partitionedBy != [] else ''))

    dfStreaming = spark.readStream.format("delta").load("/medical/bronze/" + tableName)
    dfStreaming.writeStream.option("checkpointLocation", "/medical/checkpoint/silver/" + tableName).outputMode("update").foreachBatch(appendToDelta).start()

#check streaming data copy to silver succesful
def check_streaming_data_in_silver(tableName, numRows = 5):
    print('5 rows of ' + tableName + ' table:')
    spark.read.format("delta").load("/medical/silver/" + tableName).limit(numRows).show()
    print('Total data rows:')
    spark.sql("select count(*) from delta.`/medical/silver/" + tableName + "`").show()

#cache data to mongoDB
def cache_data_to_mongoDB(goldTableName, keyTableMongoDB):
    res = spark.read.format("delta").load("/medical/gold/" + goldTableName)
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    CacheQuery.objects(key=keyTableMongoDB).delete()
    data = CacheQuery(key = keyTableMongoDB,value=results)
    data.save()

#parseQuery
def parseQuery(query, pathHDFS):
    queryRes = ''
    fromSplit = query.split('from ')
    if len(fromSplit) == 1:
        queryRes = fromSplit[0]
    else:
        fromSplit = [fromSplit[0]] + list(map(lambda a: a[:a.index(' ')] + '`' + a[a.index(' '):], fromSplit[1:]))
        queryRes = ('from delta.`' + pathHDFS).join(fromSplit)
    
    joinSplit = queryRes.split('join ')
    if len(joinSplit) == 1:
        queryRes = joinSplit[0]
    else:
        joinSplit = [joinSplit[0]] + list(map(lambda a: a[:a.index(' ')] + '`' + a[a.index(' '):], joinSplit[1:]))
        queryRes = ('join delta.`' + pathHDFS).join(joinSplit)
    return queryRes