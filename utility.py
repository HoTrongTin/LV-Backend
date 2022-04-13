from manage_schema import *
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
def startStream(project, stream):

    # Create DF schema
    schema = []
    for col in stream.columns:
        schema.append((col['name'], col['field_type'], col['nullable']))
    schema = tuple(schema)
    print('schema: ', str(schema))

    streamingSchema = StructType()
    for col in schema:
        streamingSchema.add(col[0], col[1], col[2])

    dataset_source = stream.dataset_source
    dataset_sink = stream.dataset_sink

    bronze_stream_name = "{project_name}-{folder_name}-{table_name}".format(project_name = project.name,folder_name=dataset_sink.folder_name, table_name = stream.table_name_sink)
    gold_stream_name = "{project_name}-gold-{table_name}".format(project_name = project.name, table_name = stream.table_name_sink);

    # Start Bronze & Gold streamming    
    if dataset_source.dataset_type == 'HDFS':
        streamingHDFSToBronze(project_name=project.name, schema=streamingSchema, stream=stream, stream_name=bronze_stream_name, dataset_source=dataset_source, dataset_sink=dataset_sink)
    elif dataset_source.dataset_type == 'S3':
        pass
    elif dataset_source.dataset_type == 'KAFKA':
        pass
    
    if stream.method == 'MERGE':
        streamingBronzeToGoldMergeMethod(project_name=project.name, folder_name=dataset_sink.folder_name, table_name=stream.table_name_sink, schema=schema, stream_name=gold_stream_name, mergeOn=stream.merge_on, partitionedBy=stream.partition_by)
    elif stream.method == 'APPEND':
        streamingBronzeToGoldAppendMethod(project_name=project.name, folder_name=dataset_sink.folder_name, table_name=stream.table_name_sink, schema=schema, stream_name=gold_stream_name, partitionedBy=stream.partition_by)

    # Update streamming id, name, status (ACTIVE) to MongoDB
    stream.bronze_stream_name = bronze_stream_name
    stream.gold_stream_name = gold_stream_name
    stream.bronze_stream_status = 'ACTIVE'
    stream.gold_stream_status = 'ACTIVE'

    stream.save()

    
def streamingHDFSToBronze(project_name, schema, stream, stream_name, dataset_source, dataset_sink):
    dfStreaming = spark.readStream.option("sep", ",").option("header", "true").schema(schema).csv(getHDFSStreamSource(dataset_source, stream.table_name_source)).withColumn('Date_Time', current_timestamp())
    dfStreaming.writeStream.queryName(stream_name).format('delta').outputMode("append").option("checkpointLocation", getCheckpointLocation(project_name, stream, dataset_sink)).start(getStreamSink(project_name, dataset_sink.folder_name, stream.table_name_sink))
    
def getStreamSink(project_name, folder_name, table_name):
    return "/{project_name}/{folder_name}/{table_name}".format(project_name=project_name, folder_name=folder_name, table_name=table_name)

def getHDFSStreamSource(dataset_source, table_name):
    return "/{folder_name}/{table_name}".format(folder_name=dataset_source.folder_name, table_name=table_name)

def getCheckpointLocation(project_name, stream, dataset_sink):
    return "/{project_name}/checkpoint/{folder_name}/{table_name}".format(project_name=project_name, folder_name=dataset_sink.folder_name, table_name=stream.table_name_sink)

#streaming S3 To Bronze
# def streamingS3ToBronze(project_name, table_name, schema):
#     streamingSchema = StructType()
#     for col in schema:
#         if len(col) == 2:
#             streamingSchema.add(col[0], col[1])
#         else: streamingSchema.add(col[0], col[1], col[2])

#     dfStreaming = spark.readStream.option("sep", ",").option("header", "true").schema(streamingSchema).csv(amazonS3param['s3aURL'] + "/" + project_name + "/" + table_name).withColumn('Date_Time', current_timestamp())
#     dfStreaming.writeStream.format('delta').outputMode("append").option("checkpointLocation", getCheckpointLocation(project_name, table_name)).start(getStreamSink(project_name, table_name))

#streaming Bronze To Gold With Merge Method
def streamingBronzeToGoldMergeMethod(project_name, folder_name, table_name, schema, stream_name, mergeOn, partitionedBy = []):
    def upsertToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("updates")
        
        mergeOnparser = ''
        for oncol in mergeOn:
            mergeOnparser += 'silver_' + table_name + '.' + oncol + ' = s.' + oncol + ' AND '

        microBatchOutputDF._jdf.sparkSession().sql("""
            MERGE INTO delta.`/{project_name}/silver/{table_name}` silver_{table_name}
            USING updates s
            ON """ + mergeOnparser[:-5] + """
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """.format(project_name=project_name, table_name= table_name))

    setColumns = ', '.join([' '.join(x for x in col if isinstance(x, str)) for col in schema])
    setPartitionedBy = ', '.join(partitionedBy)

    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/{project_name}/{folder_name}/{table_name}'.format(project_name=project_name, folder_name=folder_name, table_name=table_name))):
        spark.sql("""CREATE TABLE {folder_name}_{table_name} ({setColumns}, Date_Time timestamp) USING DELTA LOCATION \'/{project_name}/{folder_name}/{table_name}\'""".format(folder_name=folder_name, table_name=table_name, setColumns = setColumns, project_name=project_name))

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/{project_name}/silver/{table_name}'.format(project_name=project_name, table_name=table_name))):
        spark.sql("""CREATE TABLE silver_{table_name} ({setColumns}, Date_Time timestamp) USING DELTA LOCATION \'/{project_name}/silver/{table_name}\'""".format(table_name=table_name, setColumns = setColumns, project_name=project_name)
            + (" PARTITIONED BY (" + setPartitionedBy + ")" if partitionedBy != [] else '')
        )

    dfStreaming = spark.readStream.format("delta").load("/{project_name}/{folder_name}/{table_name}".format(project_name=project_name, folder_name=folder_name, table_name=table_name))
    dfStreaming.writeStream.queryName(stream_name).option("checkpointLocation", "/{project_name}/checkpoint/silver/{table_name}".format(project_name=project_name, table_name=table_name)) \
        .outputMode("update") \
        .foreachBatch(upsertToDelta) \
        .start()

#streaming Bronze To Gold With Append Method
def streamingBronzeToGoldAppendMethod(project_name, folder_name, table_name, schema, stream_name, partitionedBy = []):
    def appendToDelta(microBatchOutputDF, batchId): 
        microBatchOutputDF.createOrReplaceTempView("batchData")

        setFields = ', '.join([field[0] for field in schema])

        microBatchOutputDF._jdf.sparkSession().sql("""
            INSERT INTO delta.`/{project_name}/silver/{table_name}`
            ({setFields}, Date_Time)
            SELECT {setFields}, Date_Time
            FROM batchData
        """.format(project_name=project_name, table_name=table_name, setFields = setFields))

    setColumns = ', '.join([' '.join(x for x in col if isinstance(x, str)) for col in schema])
    setPartitionedBy = ', '.join(partitionedBy)

    #create bronze
    if not(DeltaTable.isDeltaTable(spark, '/{project_name}/{folder_name}/{table_name}'.format(project_name=project_name, folder_name=folder_name, table_name=table_name))):
        spark.sql("""CREATE TABLE {folder_name}_{table_name} ({setColumns}, Date_Time timestamp) USING DELTA LOCATION \'/{project_name}/{folder_name}/{table_name}\'""".format(folder_name=folder_name, table_name=table_name, setColumns = setColumns, project_name=project_name))

    #create silver
    if not(DeltaTable.isDeltaTable(spark, '/{project_name}/silver/{table_name}'.format(project_name=project_name, table_name=table_name))):
        spark.sql("""CREATE TABLE silver_{table_name} ({setColumns}, Date_Time timestamp) USING DELTA LOCATION \'/{project_name}/silver/{table_name}\'""".format(table_name=table_name, setColumns = setColumns, project_name=project_name)
            + (" PARTITIONED BY (" + setPartitionedBy + ")" if partitionedBy != [] else '')
        )

    dfStreaming = spark.readStream.format("delta").load('/{project_name}/{folder_name}/{table_name}'.format(project_name=project_name, folder_name=folder_name, table_name=table_name))
    dfStreaming.writeStream.queryName(stream_name).option("checkpointLocation", "/{project_name}/checkpoint/silver/{table_name}".format(project_name=project_name, table_name=table_name)) \
        .outputMode("update") \
        .foreachBatch(appendToDelta) \
        .start()

#cache data to mongoDB
def cache_data_to_mongoDB(project_name, key):
    res = spark.read.format("delta").load("/{project_name}/gold/gold_{key}".format(project_name=project_name, key=key))
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    CacheQuery.objects(key=project_name + "_" + key).delete()
    data = CacheQuery(key=project_name + "_" + key,value=results)
    data.save()

#parseQuery
def parseQuery(query, pathHDFS):
    queryRes = ''
    fromSplit = query.split('from ')
    if len(fromSplit) == 1:
        queryRes = fromSplit[0]
    else:
        queryRes = fromSplit[0]
        for itemSplit in fromSplit[1:]:
            if itemSplit[0] != '(':
                queryRes += 'from delta.`' + pathHDFS + (itemSplit[:itemSplit.index(' ') - 1] + '`)' if itemSplit[itemSplit.index(' ') - 1] == ')' else itemSplit[:itemSplit.index(' ')] + '`') + itemSplit[itemSplit.index(' '):]
            else: queryRes += 'from ' + itemSplit
    
    joinSplit = queryRes.split('join ')
    if len(joinSplit) == 1:
        queryRes = joinSplit[0]
    else:
        queryRes = joinSplit[0]
        for itemSplit in joinSplit[1:]:
            if itemSplit[0] != '(':
                queryRes += 'join delta.`' + pathHDFS + (itemSplit[:itemSplit.index(' ') - 1] + '`)' if itemSplit[itemSplit.index(' ') -1] == ')' else itemSplit[:itemSplit.index(' ')] + '`') + itemSplit[itemSplit.index(' '):]
            else: queryRes += 'join ' + itemSplit
            
    return queryRes

#cache data to Gold
def cache_gold_analysis_query(project_name, sql, key):
    pathHDFSsilver = "/{project_name}/silver/".format(project_name=project_name)
    sqlFormatted = parseQuery(sql, pathHDFSsilver)
    res = spark.sql(sqlFormatted)
    print(sql)
    res.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/{project_name}/gold/gold_{key}".format(project_name=project_name, key=key))