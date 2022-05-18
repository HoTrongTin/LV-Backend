import tensorflow as tf
import pandas as pd
from sklearn.metrics import confusion_matrix
from sparkSetup import spark

#Function to check result with input prob which classifier one patient be or not be sick
# def check(classifier, X_predict, X_train, y_train, X_test, y_test, prob):
#   testPredict = classifier.predict(X_predict)
#   testTrain = classifier.predict(X_train)
#   testTest = classifier.predict(X_test)
#   m = len(testPredict[testPredict >= prob])
#   a = len(testTrain[testTrain >= prob]) / len(y_train[y_train == 1])
#   b = len(testTest[testTest >= prob]) / len(y_test[y_test == 1])
#   print(prob, m, a, b)

def predictANN(prob = 0.28, filename = "data0405_ver4.csv"):
    classifier = tf.keras.models.load_model('model_classifier/ANN_classifier')

    # Check its architecture
    classifier.summary()

    predict_data = pd.read_csv('dataML/dataPredict/'+filename)
    numRows = predict_data.shape[0]

    X_predict = predict_data.iloc[:,1:-1]

    result = classifier.predict(X_predict)
    result = result.reshape(numRows)
    for i in range(len(result)):
        if result[i] >= prob:
            result[i] = 1
        else: result[i] = 0

    #Check accuracy
    tn, fp, fn, tp = confusion_matrix(predict_data['label'].tolist(), result).ravel()
    print(tn, fp, fn, tp)
    print('Accuracy: {0}'.format((tp+tn)/numRows))
    print('True Positive Rate (TPR): {0}'.format(tp/(tp+fn)))
    print('Positive Predictive Value (PPV): {0}'.format(tp/(tp+fp)))
    print('Negative Predictive Value (NPV): {0}'.format(tn/(tn+fn)))
    print('False Negative Rate (FNR): {0}'.format(fn/(tp+fn)))
    print('False Positive Rate (FPR): {0}'.format(fp/(fp+tn)))

    datasetRes = pd.DataFrame({'hadm_id': predict_data['hadm_id'], 'prediction': result})
    datasetRes.to_csv('dataML/predictRes/result_ANN_' + filename,index=False)
    return spark.createDataFrame(datasetRes.iloc[:5,:])