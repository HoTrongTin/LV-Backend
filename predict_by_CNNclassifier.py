import tensorflow as tf
import pandas as pd

#Function to check result with input prob which classifier one patient be or not be sick
# def check(classifier, X_predict, X_train, y_train, X_test, y_test, prob):
#   testPredict = classifier.predict(X_predict)
#   testTrain = classifier.predict(X_train)
#   testTest = classifier.predict(X_test)
#   m = len(testPredict[testPredict >= prob])
#   a = len(testTrain[testTrain >= prob]) / len(y_train[y_train == 1])
#   b = len(testTest[testTest >= prob]) / len(y_test[y_test == 1])
#   print(prob, m, a, b)

def predict(prob = 0.28):
    classifier = tf.keras.models.load_model('model_classifier/CNN_classifier')

    # Check its architecture
    classifier.summary()

    predict_data = pd.read_csv('model_classifier/cleaned_data_test.csv')

    X_predict = predict_data.iloc[:,1:]
    X_predict = X_predict.to_numpy()
    X_predict = X_predict.reshape(10234,1,171,1)

    # i = 0.3
    # while i <= 0.4:
    #     check(X_predict, X_under_train, y_under, X_test, y_test, i)
    #     i += 0.01

    result = classifier.predict(X_predict)
    result = result.reshape(10234)
    for i in range(len(result)):
        if result[i] >= prob:
            result[i] = 1
        else: result[i] = 0

    q = pd.read_csv('model_classifier/submission.csv')
    encounter_id = q['encounter_id']
    dataset = pd.DataFrame({'encounter_id': encounter_id, 'diabetes_mellitus': result})
    return dataset