from cronjobSilverToGold import *
from cronjobGoldToMongoDB import *

#init Schedule jobs
def cron_check_streaming():
    print('CronJob for checking streaming...')
    check_streaming_data_in_silver("d_patients")
    check_streaming_data_in_silver("admissions")
    check_streaming_data_in_silver("drgevents")
    check_streaming_data_in_silver("d_codeditems")
    check_streaming_data_in_silver("demographic_detail")
    check_streaming_data_in_silver("icd9")
    check_streaming_data_in_silver("icustay_days")
    check_streaming_data_in_silver("icustay_detail")
    # check_streaming_data_in_silver("chartevents")
    # check_streaming_data_in_silver("icustayevents")
    # check_streaming_data_in_silver("ioevents")
    # check_streaming_data_in_silver("labevents")
    # check_streaming_data_in_silver("medevents")


def cron_data_to_Gold():
    print('Schedule jobs copy data from silver to Gold...')
    # cache_gold_analysis_patients_by_age()
    # cache_gold_analysis_admissions_and_deied_patients_in_hospital()
    # cache_gold_analysis_get_5_common_diseases_by_month()
    # cache_gold_analysis_diseases_affect_stay_days()
    # cache_gold_analysis_20_common_diseases_clinical_results()
    # cache_gold_analysis_state_affect_total_died_patients()
    # cache_gold_analysis_patients_by_sex()
    # cache_gold_analysis_patients_died_in_hospital()
    # cache_gold_analysis_diseases_clinical_affected_died_patients()
    # cache_gold_analysis_diseases_clinical_by_month()

def cron_data_to_mongoDB():
    print('Setup CronJob for copying data from gold to mongoDB...')
    # cache_mongoDB_analysis_patients_by_age()
    # cache_mongoDB_analysis_admissions_and_deied_patients_in_hospital()
    # cache_mongoDB_analysis_get_5_common_diseases_by_month()
    # cache_mongoDB_analysis_diseases_affect_stay_days()
    # cache_mongoDB_analysis_20_common_diseases_clinical_results()
    # cache_mongoDB_analysis_state_affect_total_died_patients()
    # cache_mongoDB_analysis_patients_by_sex()
    # cache_mongoDB_analysis_patients_died_in_hospital()
    # cache_mongoDB_analysis_diseases_clinical_affected_died_patients()
    # cache_mongoDB_analysis_diseases_clinical_by_month()