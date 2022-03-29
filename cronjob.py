from sparkSetup import spark

#Schedule jobs
def merge_silver_d_patients():
    spark.sql("""
MERGE INTO delta.`/medical/silver/d_patients` silver_d_patients
USING (select * from delta.`/medical/bronze/d_patients`
where Date_Time > (select CASE WHEN max(Date_Time) is not NULL THEN max(Date_Time) ELSE '2000-01-01 00:00:00' END from delta.`/medical/bronze/d_patients`)
) updates
ON silver_d_patients.subject_id = updates.subject_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
""")

