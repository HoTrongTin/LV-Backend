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