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

vecAssembler = VectorAssembler(\
                               outputCol = "features") \
                              .setHandleInvalid("skip")
vecAssembler.setInputCols(['age', 'weight', 'gender_M', 'platelets', 'spo2', 'creatinine', 'hematocrit', 'aids', 'lymphoma', 'solid_tumor_with_metastasis', 'heartrate', 'calcium', 'wbc', 'glucose', 'inr', 'potassium', 'sodium', 'ethnicity'])

from pyspark.sql.types import StructType
schema = StructType() \
      .add("age",FloatType(),True) \
      .add("weight",FloatType(),True) \
      .add("gender_M",IntegerType(),True) \
      .add("platelets",FloatType(),True) \
      .add("spo2",FloatType(),True) \
      .add("creatinine",FloatType(),True) \
      .add("hematocrit",FloatType(),True) \
      .add("aids",IntegerType(),True) \
      .add("lymphoma",IntegerType(),True) \
      .add("solid_tumor_with_metastasis",IntegerType(),True) \
      .add("heartrate",FloatType(),True) \
      .add("calcium",FloatType(),True) \
      .add("wbc",FloatType(),True) \
      .add("glucose",FloatType(),True) \
      .add("inr",FloatType(),True) \
      .add("potassium",FloatType(),True) \
      .add("sodium",FloatType(),True) \
      .add("ethnicity",IntegerType(),True) \
      .add("label",FloatType(),True)

trainDF = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("dataTrain0405.csv")

stdScaler = StandardScaler(inputCol="features", \
                        outputCol="scaledFeatures", \
                        withStd=True, \
                        withMean=False)
# Optional - as we're using Pipeline
# Compute summary statistics by fitting the StandardScaler
scalerModel = stdScaler.fit(vecTrainDF)
# Normalize each feature to have unit standard deviation.
scaledDataDF = scalerModel.transform(vecTrainDF)


def saveModels():
    # 1. LogisticRegression
    lr = LogisticRegression(maxIter=100, \
                        featuresCol="scaledFeatures", \
                        family = "binomial", \
                        labelCol="label")
    pipeline_lr = Pipeline(stages=[vecAssembler, stdScaler, lr])
    pipelineModel_lr = pipeline_lr.fit(trainDF)
    pipelineModel_lr.write().overwrite().save("model_classifier/FMClassifier")

    # 2.LinearSVC
    lsvc = LinearSVC(maxIter=10, \
                 regParam=0.1, \
                 featuresCol="scaledFeatures", \
                 labelCol="label")
    pipeline_lsvc = Pipeline(stages=[vecAssembler, stdScaler, lsvc])
    pipelineModel_lsvc = pipeline_lsvc.fit(trainDF)
    pipelineModel_lsvc.write().overwrite().save("model_classifier/LinearSVC")

    # 3. NaiveBayes
    nb = NaiveBayes(smoothing=1.0, \
                modelType="gaussian", \
                featuresCol="scaledFeatures", \
                labelCol="label")
    pipeline_nb = Pipeline(stages=[vecAssembler, stdScaler, nb])
    pipelineModel_nb = pipeline_nb.fit(trainDF)
    pipelineModel_nb.write().overwrite().save("model_classifier/NaiveBayes")

    # 4. DecisionTreeClassifier
    dt = DecisionTreeClassifier(labelCol="label", \
                            featuresCol="scaledFeatures", \
                            impurity="gini")
    pipeline_dt = Pipeline(stages=[vecAssembler, stdScaler, dt])
    pipelineModel_dt = pipeline_dt.fit(trainDF)
    pipelineModel_dt.write().overwrite().save("model_classifier/DecisionTreeClassifier")

    # 5 . RandomForestClassifier
    rf = RandomForestClassifier(labelCol="label", \
                            featuresCol="scaledFeatures", \
                            numTrees=50)
    pipeline_rf = Pipeline(stages=[vecAssembler, stdScaler, rf])
    pipelineModel_rf = pipeline_rf.fit(trainDF)
    pipelineModel_rf.write().overwrite().save("model_classifier/RandomForestClassifier")

    # 6 . GBTClassifier
    gbt = GBTClassifier(labelCol="label", \
                    featuresCol="scaledFeatures", \
                    maxIter=10)
    pipeline_gbt = Pipeline(stages=[vecAssembler, stdScaler, gbt])
    pipelineModel_gbt = pipeline_gbt.fit(trainDF)
    pipelineModel_gbt.write().overwrite().save("model_classifier/GBTClassifier")

    # 7 . MultilayerPerceptronClassifier
    layers = [18, 5, 4, 2]
    # create the trainer and set its parameters
    mlp = MultilayerPerceptronClassifier(labelCol="label", \
                                        featuresCol="scaledFeatures", \
                                        maxIter=100, layers=layers, \
                                        blockSize=128, \
                                        seed=1234)
    pipeline_mlp = Pipeline(stages=[vecAssembler, stdScaler, mlp])
    pipelineModel_mlp = pipeline_mlp.fit(trainDF)
    pipelineModel_mlp.write().overwrite().save("model_classifier/MultilayerPerceptronClassifier")

    # 8 . OneVsRest
    ovr = OneVsRest(classifier=lr, \
                labelCol="label", \
                featuresCol="scaledFeatures")
    pipeline_ovr = Pipeline(stages=[vecAssembler, stdScaler, ovr])
    pipelineModel_ovr = pipeline_ovr.fit(trainDF)
    pipelineModel_ovr.write().overwrite().save("model_classifier/OneVsRest")

    # 9 . FMClassifier
    fm = FMClassifier(labelCol="label", \
                  featuresCol="scaledFeatures", \
                  stepSize=0.001)
    pipeline_fm = Pipeline(stages=[vecAssembler, stdScaler, fm])
    pipelineModel_fm = pipeline_fm.fit(trainDF)
    pipelineModel_fm.write().overwrite().save("model_classifier/FMClassifier")