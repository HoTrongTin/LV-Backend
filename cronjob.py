from sparkSetup import spark
from mongodb import CacheQuery
import json

#Schedule jobs
def cache_test_streaming_d_patients_bronze():
    res = spark.read.format("delta").load("/medical/bronze/d_patients")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    
    data = CacheQuery(key='cache_test_streaming_d_patients_bronze',value=results)
    data.save()

def cache_test_streaming_d_patients_silver():
    res = spark.read.format("delta").load("/medical/silver/d_patients")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    
    data = CacheQuery(key='cache_test_streaming_d_patients_silver',value=results)
    data.save()

def cache_test_streaming_admissions_bronze():
    res = spark.read.format("delta").load("/medical/bronze/admissions")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    
    data = CacheQuery(key='cache_test_streaming_admissions_bronze',value=results)
    data.save()

def cache_test_streaming_admissions_silver():
    res = spark.read.format("delta").load("/medical/silver/admissions")
    res.show()
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    
    data = CacheQuery(key='cache_test_streaming_admissions_silver',value=results)
    data.save()

#Schedule jobs
def cron_cache_query():
    print('Cron job running...')
    print('Bronze d_patients...')
    cache_test_streaming_d_patients_bronze()
    print('Silver d_patients...')
    cache_test_streaming_d_patients_silver()
    print('Bronze admissions...')
    cache_test_streaming_admissions_bronze()
    print('Silver admissions...')
    cache_test_streaming_admissions_silver()