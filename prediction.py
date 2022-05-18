from sparkSetup import spark
import pandas as pd
from pyspark.ml import PipelineModel
from sklearn.metrics import confusion_matrix

def accuracy(resDF):
    label = resDF.select('label').rdd.flatMap(lambda x: x).collect()
    predict = resDF.select('prediction').rdd.flatMap(lambda x: x).collect()
    tn, fp, fn, tp = confusion_matrix(label, predict).ravel()
    print(tn, fp, fn, tp)
    print('Accuracy: {0}'.format((tp+tn)/len(label)))
    print('True Positive Rate (TPR): {0}'.format(tp/(tp+fn)))
    print('Positive Predictive Value (PPV): {0}'.format(tp/(tp+fp)))
    print('Negative Predictive Value (NPV): {0}'.format(tn/(tn+fn)))
    print('False Negative Rate (FNR): {0}'.format(fn/(tp+fn)))
    print('False Positive Rate (FPR): {0}'.format(fp/(fp+tn)))

def predict(algorithm, filename = "data0405_ver4.csv"):
      testPandasDF = pd.read_csv('dataML/dataPredict/'+filename)
      testDF = spark.createDataFrame(testPandasDF)

      pipeline = PipelineModel.load("model_classifier/" + algorithm)
      predRes = pipeline.transform(testDF)

      accuracy(predRes)
      print('---------------------------------------------------')
      datasetRes = predRes.select("hadm_id", "prediction").toPandas()
      datasetRes.to_csv('dataML/predictRes/result_' + algorithm + '_' + filename,index=False)

      return spark.createDataFrame(datasetRes.iloc[:5,:])