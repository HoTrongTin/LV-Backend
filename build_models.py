from sparkSetup import spark
from pyspark.sql.types import StructType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.classification import OneVsRest
from pyspark.ml.classification import FMClassifier

def buildModels():
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

      trainDF = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/dataML/dataTrain0405.csv")

      vecAssembler = VectorAssembler(\
                                    outputCol = "features") \
                                    .setHandleInvalid("skip")
      vecAssembler.setInputCols(['age', 'weight', 'gender_M', 'platelets', 'spo2', 'creatinine', 'hematocrit', 'aids', 'lymphoma', 'solid_tumor_with_metastasis', 'heartrate', 'calcium', 'wbc', 'glucose', 'inr', 'potassium', 'sodium', 'ethnicity'])


      stdScaler = StandardScaler(inputCol="features", \
                              outputCol="scaledFeatures", \
                              withStd=True, \
                              withMean=False)
      # 1. LogisticRegression
      lr = LogisticRegression(maxIter=100, \
                        elasticNetParam=1, \
                        family = "binomial", \
                        labelCol="label")
      pipeline_lr = Pipeline(stages=[vecAssembler, stdScaler, lr])
      pipelineModel_lr = pipeline_lr.fit(trainDF)
      pipelineModel_lr.write().overwrite().save("model_classifier/LogisticRegression")
      print('Built LogisticRegression model successful!')

      # 2.LinearSVC
      lsvc = LinearSVC(maxIter=10, \
                 featuresCol="scaledFeatures", \
                 labelCol="label")
      pipeline_lsvc = Pipeline(stages=[vecAssembler, stdScaler, lsvc])
      pipelineModel_lsvc = pipeline_lsvc.fit(trainDF)
      pipelineModel_lsvc.write().overwrite().save("model_classifier/LinearSVC")
      print('Built LinearSVC model successful!')

      # 3. NaiveBayes
      nb = NaiveBayes(smoothing=0, \
                modelType="gaussian", \
                featuresCol="scaledFeatures", \
                labelCol="label")
      pipeline_nb = Pipeline(stages=[vecAssembler, stdScaler, nb])
      pipelineModel_nb = pipeline_nb.fit(trainDF)
      pipelineModel_nb.write().overwrite().save("model_classifier/NaiveBayes")
      print('Built NaiveBayes model successful!')

      # 4. DecisionTreeClassifier
      dt = DecisionTreeClassifier(labelCol="label", \
                            featuresCol="scaledFeatures", \
                            impurity="gini", maxDepth = 30)
      pipeline_dt = Pipeline(stages=[vecAssembler, stdScaler, dt])
      pipelineModel_dt = pipeline_dt.fit(trainDF)
      pipelineModel_dt.write().overwrite().save("model_classifier/DecisionTreeClassifier")
      print('Built DecisionTreeClassifier model successful!')

      # 5 . RandomForestClassifier
      rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="scaledFeatures", maxDepth = 30,\
                            numTrees=1)
      pipeline_rf = Pipeline(stages=[vecAssembler, stdScaler, rf])
      pipelineModel_rf = pipeline_rf.fit(trainDF)
      pipelineModel_rf.write().overwrite().save("model_classifier/RandomForestClassifier")
      print('Built RandomForestClassifier model successful!')

      # 6 . GBTClassifier
      gbt = GBTClassifier(labelCol="label", \
                    featuresCol="scaledFeatures", maxDepth = 30,\
                    maxIter=20)
      pipeline_gbt = Pipeline(stages=[vecAssembler, stdScaler, gbt])
      pipelineModel_gbt = pipeline_gbt.fit(trainDF)
      pipelineModel_gbt.write().overwrite().save("model_classifier/GBTClassifier")
      print('Built GBTClassifier model successful!')

      # 7 . MultilayerPerceptronClassifier
      layers = [18, 16, 8, 2]
      # create the trainer and set its parameters
      mlp = MultilayerPerceptronClassifier(labelCol="label", \
                                     featuresCol="scaledFeatures", \
                                     maxIter=1, layers=layers, \
                                     blockSize=128, \
                                     seed=1234)
      pipeline_mlp = Pipeline(stages=[vecAssembler, stdScaler, mlp])
      pipelineModel_mlp = pipeline_mlp.fit(trainDF)
      pipelineModel_mlp.write().overwrite().save("model_classifier/MultilayerPerceptronClassifier")
      print('Built MultilayerPerceptronClassifier model successful!')

      # 8 . OneVsRest
      lr_ovr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)
      ovr = OneVsRest(classifier=lr_ovr, \
                  labelCol="label", \
                  featuresCol="scaledFeatures")
      pipeline_ovr = Pipeline(stages=[vecAssembler, stdScaler, ovr])
      pipelineModel_ovr = pipeline_ovr.fit(trainDF)
      pipelineModel_ovr.write().overwrite().save("model_classifier/OneVsRest")
      print('Built OneVsRest model successful!')

      # 9 . FMClassifier
      fm = FMClassifier(labelCol="label", \
                  featuresCol="scaledFeatures", \
                  stepSize=0.01)
      pipeline_fm = Pipeline(stages=[vecAssembler, stdScaler, fm])
      pipelineModel_fm = pipeline_fm.fit(trainDF)
      pipelineModel_fm.write().overwrite().save("model_classifier/FMClassifier")
      print('Built FMClassifier model successful!')