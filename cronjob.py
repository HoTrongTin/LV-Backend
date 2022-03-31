from sparkSetup import spark
from mongodb import CacheQuery
import numpy as np
import pandas as pd
import json

#CronJob for checking streaming
def check_streaming_d_patients_bronze():
    res = spark.read.format("delta").load("/medical/bronze/d_patients")
    res.show()

def check_streaming_d_patients_silver():
    res = spark.read.format("delta").load("/medical/silver/d_patients")
    res.show()

def check_streaming_admissions_bronze():
    res = spark.read.format("delta").load("/medical/bronze/admissions")
    res.show()

def check_streaming_admissions_silver():
    res = spark.read.format("delta").load("/medical/silver/admissions")
    res.show()

#Schedule jobs copy data from silver to Gold
def cache_gold_analysis_patients_by_age():
    res = spark.sql("""
    select period, count(*) num from
    (select 
    case 
        WHEN age < 2 THEN 'Infancy' 
        WHEN age >= 2 and age < 6 THEN 'Early Childhood' 
        WHEN age >= 6 and age < 12 THEN 'Later Childhood'
        WHEN age >= 12 and age < 20 THEN 'Adolescence'
        WHEN age >= 20 and age < 40 THEN 'Young adult'
        WHEN age >= 40 and age < 60 THEN 'Middle-aged'
        ELSE 'Senior Citizen' 
    END as period from
    (select extract(day from dod - dob)/365 as age from delta.`/medical/silver/d_patients`) age) period
    group by period
    order by num desc
    """)
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_patients_by_age")

def cache_gold_analysis_admissions_and_deied_patients_in_hospital():
    res = spark.sql("""
    select year(admit_dt) year, count(*) num from delta.`/medical/silver/admissions` as admissions
    where year(admit_dt) >= 2005 and year(admit_dt) <= 2015
    group by year(admit_dt)
    order by year(admit_dt)
    """).toPandas()

    dfdied = spark.sql("""
    select year(dod) year, count(*) num from delta.`/medical/silver/d_patients` as d_patients
    where year(dod) >= 2005 and year(dod) <= 2015 and hospital_expire_flg = 'Y'
    group by year(dod)
    order by year(dod)
    """).toPandas()
    
    res['numDied'] = dfdied['num']
    res = spark.createDataFrame(res)
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_admissions_and_deied_patients_in_hospital")

def cache_gold_analysis_get_5_common_diseases_by_month():
    res = spark.sql("""
select * from 
(select ROW_NUMBER() OVER(PARTITION BY month ORDER BY num desc) 
    AS ROW_NUMBER, * from
(select drgevents.itemid, description, month(admit_dt) as month, count(*) as num 
from delta.`/medical/silver/drgevents` as drgevents
join delta.`/medical/silver/d_codeditems` as d_codeditems
join delta.`/medical/silver/admissions` as admissions
on drgevents.itemid = d_codeditems.itemid and admissions.hadm_id = drgevents.hadm_id
group by drgevents.itemid, description, month
order by num desc) tmp) tmp1
where ROW_NUMBER < 6""")
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_get_5_common_diseases_by_month")

def cache_gold_analysis_diseases_affect_stay_days():
    res = spark.sql("""
select itemid, description, avg(stay_days) as avgStayDays from
(select EXTRACT( DAY FROM (disch_dt - admit_dt)) as stay_days, description, drgevents.itemid 
from delta.`/medical/silver/drgevents` as drgevents 
join delta.`/medical/silver/admissions` as admissions
join delta.`/medical/silver/d_codeditems` as d_codeditems
on d_codeditems.itemid = drgevents.itemid 
and drgevents.hadm_id = admissions.hadm_id) tmp
group by itemid, description
order by avg(stay_days) desc
""")
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_diseases_affect_stay_days")

def cache_gold_analysis_20_common_diseases_clinical_results():
    res = spark.sql("""
select drgevents.itemid, description, count(*) as numCases
from delta.`/medical/silver/drgevents` as drgevents 
join delta.`/medical/silver/d_codeditems` as d_codeditems 
on drgevents.itemid = d_codeditems.itemid
group by drgevents.itemid, description, type
order by numCases desc
LIMIT 20
""")
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_20_common_diseases_clinical_results")

def cache_gold_analysis_state_affect_total_died_patients():
    totalpatientsURGENT = spark.sql("""
    select COUNT(*) from
    (SELECT ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY hadm_id desc) 
        AS ROW_NUMBER, subject_id, admission_type_descr 
    from delta.`/medical/silver/demographic_detail`) tableLastest 
    join delta.`/medical/silver/d_patients` d_patients
    on d_patients.subject_id = tableLastest.subject_id where ROW_NUMBER = 1 and admission_type_descr = 'URGENT'
    """).first()['count(1)']

    patientsURGENTDiedInHospital = spark.sql("""
    select COUNT(*) from
    (SELECT ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY hadm_id desc) 
        AS ROW_NUMBER, subject_id, admission_type_descr 
    from delta.`/medical/silver/demographic_detail`) tableLastest 
    join delta.`/medical/silver/d_patients` d_patients
    on d_patients.subject_id = tableLastest.subject_id where ROW_NUMBER = 1 and admission_type_descr = 'URGENT' and hospital_expire_flg = 'Y'
    """).first()['count(1)']

    totalpatientsEMERGENCY = spark.sql("""
    select COUNT(*) from
    (SELECT ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY hadm_id desc) 
        AS ROW_NUMBER, subject_id, admission_type_descr 
    from delta.`/medical/silver/demographic_detail`) tableLastest 
    join delta.`/medical/silver/d_patients` d_patients
    on d_patients.subject_id = tableLastest.subject_id where ROW_NUMBER = 1 and admission_type_descr = 'EMERGENCY'
    """).first()['count(1)']

    patientsEMERGENCYDiedInHospital = spark.sql("""
    select COUNT(*) from
    (SELECT ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY hadm_id desc) 
        AS ROW_NUMBER, subject_id, admission_type_descr 
    from delta.`/medical/silver/demographic_detail`) tableLastest 
    join delta.`/medical/silver/d_patients` d_patients
    on d_patients.subject_id = tableLastest.subject_id where ROW_NUMBER = 1 and admission_type_descr = 'EMERGENCY' and hospital_expire_flg = 'Y'
    """).first()['count(1)']

    df = np.array([['URGENT', totalpatientsURGENT, patientsURGENTDiedInHospital, patientsURGENTDiedInHospital/totalpatientsURGENT]\
                ,['EMERGENCY', totalpatientsEMERGENCY, patientsEMERGENCYDiedInHospital, patientsEMERGENCYDiedInHospital/totalpatientsEMERGENCY]])
    res = spark.createDataFrame(pd.DataFrame(df, columns = ['state','total_patients','total_died_patients','ratio']))
    res.write.format("delta").mode("overwrite").save("/medical/gold/cache_gold_analysis_state_affect_total_died_patients")

#Setup CronJob for copying data from gold to mongoDB
def cache_mongoDB_analysis_patients_by_age():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_patients_by_age")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_patients_by_age',value=results)
    data.save()

def cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_admissions_and_deied_patients_in_hospital")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital',value=results)
    data.save()

def cache_mongoDB_analysis_get_5_common_diseases_by_month():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_get_5_common_diseases_by_month")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_get_5_common_diseases_by_month',value=results)
    data.save()

def cache_mongoDB_analysis_diseases_affect_stay_days():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_diseases_affect_stay_days")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_diseases_affect_stay_days',value=results)
    data.save()

def cache_mongoDB_analysis_20_common_diseases_clinical_results():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_20_common_diseases_clinical_results")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_20_common_diseases_clinical_results',value=results)
    data.save()

def cache_mongoDB_analysis_state_affect_total_died_patients():
    res = spark.read.format("delta").load("/medical/gold/cache_gold_analysis_state_affect_total_died_patients")
    results = res.toJSON().map(lambda j: json.loads(j)).collect()
    data = CacheQuery(key='cache_mongoDB_analysis_state_affect_total_died_patients',value=results)
    data.save()

#Schedule jobs
def cron_check_streaming():
    print('CronJob for checking streaming...')
    check_streaming_d_patients_bronze()
    print('Silver d_patients...')
    check_streaming_d_patients_silver()
    print('Bronze admissions...')
    check_streaming_admissions_bronze()
    print('Silver admissions...')
    check_streaming_admissions_silver()

def cron_data_to_Gold():
    print('Schedule jobs copy data from silver to Gold...')
    cache_gold_analysis_patients_by_age()
    cache_gold_analysis_admissions_and_deied_patients_in_hospital()
    cache_gold_analysis_get_5_common_diseases_by_month()
    cache_gold_analysis_diseases_affect_stay_days()
    cache_gold_analysis_20_common_diseases_clinical_results()
    cache_gold_analysis_state_affect_total_died_patients()

def cron_data_to_mongoDB():
    print('Setup CronJob for copying data from gold to mongoDB...')
    cache_mongoDB_analysis_patients_by_age()
    cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital()
    cache_mongoDB_analysis_get_5_common_diseases_by_month()
    cache_mongoDB_analysis_diseases_affect_stay_days()
    cache_mongoDB_analysis_20_common_diseases_clinical_results()
    cache_mongoDB_analysis_state_affect_total_died_patients()