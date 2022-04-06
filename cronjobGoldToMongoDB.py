from sparkSetup import spark
import numpy as np
import pandas as pd
from utility import *

#Setup CronJob for copying data from gold to mongoDB
def cache_mongoDB_analysis_patients_by_age():
    cache_data_to_mongoDB("cache_gold_analysis_patients_by_age", "cache_mongoDB_analysis_patients_by_age")

def cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital():
    cache_data_to_mongoDB("cache_gold_analysis_admissions_and_deied_patients_in_hospital", "cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital")

def cache_mongoDB_analysis_get_5_common_diseases_by_month():
    cache_data_to_mongoDB("cache_gold_analysis_get_5_common_diseases_by_month", "cache_mongoDB_analysis_get_5_common_diseases_by_month")

def cache_mongoDB_analysis_diseases_affect_stay_days():
    cache_data_to_mongoDB("cache_gold_analysis_diseases_affect_stay_days", "cache_mongoDB_analysis_diseases_affect_stay_days")

def cache_mongoDB_analysis_20_common_diseases_clinical_results():
    cache_data_to_mongoDB("cache_gold_analysis_20_common_diseases_clinical_results", "cache_mongoDB_analysis_20_common_diseases_clinical_results")

def cache_mongoDB_analysis_state_affect_total_died_patients():
    cache_data_to_mongoDB("cache_gold_analysis_state_affect_total_died_patients", "cache_mongoDB_analysis_state_affect_total_died_patients")

def cache_mongoDB_analysis_patients_by_sex():
    cache_data_to_mongoDB("cache_gold_analysis_patients_by_sex", "cache_mongoDB_analysis_patients_by_sex")

def cache_mongoDB_analysis_patients_died_in_hospital():
    cache_data_to_mongoDB("cache_gold_analysis_patients_died_in_hospital", "cache_mongoDB_analysis_patients_died_in_hospital")

def cache_mongoDB_analysis_diseases_clinical_affected_died_patients():
    cache_data_to_mongoDB("cache_gold_analysis_diseases_clinical_affected_died_patients", "cache_mongoDB_analysis_diseases_clinical_affected_died_patients")

def cache_mongoDB_analysis_diseases_clinical_by_month():
    cache_data_to_mongoDB("cache_gold_analysis_diseases_clinical_by_month", "cache_mongoDB_analysis_diseases_clinical_by_month")