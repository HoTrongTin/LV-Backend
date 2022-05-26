from sparkSetup import spark
import numpy as np
import pandas as pd
from utility import *

#Setup CronJob for copying data from gold to mongoDB
def cache_mongoDB_analyze_patients_by_age():
    cache_data_to_mongoDB("medical", "analyze_patients_by_age")

def cache_mongoDB_analyze_admissions_and_deied_patients_in_hospital():
    cache_data_to_mongoDB("medical", "analyze_admissions_and_deied_patients_in_hospital")

def cache_mongoDB_analyze_get_5_common_diseases_by_month():
    cache_data_to_mongoDB("medical", "analyze_get_5_common_diseases_by_month")

def cache_mongoDB_analyze_diseases_affect_stay_days():
    cache_data_to_mongoDB("medical", "analyze_diseases_affect_stay_days")

def cache_mongoDB_analyze_20_common_diseases_clinical_results():
    cache_data_to_mongoDB("medical", "analyze_20_common_diseases_clinical_results")

def cache_mongoDB_analyze_state_affect_total_died_patients():
    cache_data_to_mongoDB("medical", "analyze_state_affect_total_died_patients")

def cache_mongoDB_analyze_patients_by_sex():
    cache_data_to_mongoDB("medical", "analyze_patients_by_sex")

def cache_mongoDB_analyze_patients_died_in_hospital():
    cache_data_to_mongoDB("medical", "analyze_patients_died_in_hospital")

def cache_mongoDB_analyze_diseases_clinical_affected_died_patients():
    cache_data_to_mongoDB("medical", "analyze_diseases_clinical_affected_died_patients")

def cache_mongoDB_analyze_diseases_clinical_by_month():
    cache_data_to_mongoDB("medical", "analyze_diseases_clinical_by_month")