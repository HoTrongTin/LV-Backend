from sparkSetup import spark
from pyspark.sql.types import StructType
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def predict(algorithm):
      testDF = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/dataML/data0405_ver4.csv")

      evaluator = MulticlassClassificationEvaluator( \
                  labelCol="label", \
                  predictionCol="prediction", \
                  metricName="accuracy")

      schema = StructType() \
            .add("age","float",True) \
            .add("weight","float",True) \
            .add("gender_M","integer",True) \
            .add("platelets","float",True) \
            .add("spo2","float",True) \
            .add("creatinine","float",True) \
            .add("hematocrit","float",True) \
            .add("aids","integer",True) \
            .add("lymphoma","integer",True) \
            .add("solid_tumor_with_metastasis","integer",True) \
            .add("heartrate","float",True) \
            .add("calcium","float",True) \
            .add("wbc","float",True) \
            .add("glucose","float",True) \
            .add("inr","float",True) \
            .add("potassium","float",True) \
            .add("sodium","float",True) \
            .add("ethnicity","integer",True) \
            .add("label","float",True)

      pipeline = PipelineModel.load("model_classifier/" + algorithm)
      predRes = pipeline.transform(testDF)

      accuracy = evaluator.evaluate(predRes)
      print("Accuracy of LogisticRegression is = %g"%(accuracy))
      print("Test Error of LogisticRegression = %g "%(1.0 - accuracy))
      print('---------------------------------------------------')

      return predRes.select("label", "prediction").limit(5)