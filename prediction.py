from sparkSetup import spark
from pyspark.sql.types import StructType
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from sklearn.metrics import confusion_matrix

def accuracy(resDF):
    label = resDF.select('label').rdd.flatMap(lambda x: x).collect()
    predict = resDF.select('prediction').rdd.flatMap(lambda x: x).collect()
    tn, fp, fn, tp = confusion_matrix(label, predict).ravel()
    print(tn, fp, fn, tp)
    print('Accuracy: {0}'.format((tp+tn)/4029))
    print('True Positive Rate (TPR): {0}'.format(tp/(tp+fn)))
    print('Positive Predictive Value (PPV): {0}'.format(tp/(tp+fp)))
    print('Negative Predictive Value (NPV): {0}'.format(tn/(tn+fn)))
    print('False Negative Rate (FNR): {0}'.format(fn/(tp+fn)))
    print('False Positive Rate (FPR): {0}'.format(fp/(fp+tn)))

def predict(algorithm):
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
            
      testDF = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/dataML/data0405_ver4.csv")

      evaluator = MulticlassClassificationEvaluator( \
                  labelCol="label", \
                  predictionCol="prediction", \
                  metricName="accuracy")

      pipeline = PipelineModel.load("model_classifier/" + algorithm)
      predRes = pipeline.transform(testDF)

      accuracy = evaluator.evaluate(predRes)
      print("Accuracy of LogisticRegression is = %g"%(accuracy))
      print("Test Error of LogisticRegression = %g "%(1.0 - accuracy))
      print('---------------------------------------------------')
      accuracy(predRes)

      return predRes.select("label", "prediction").limit(5)