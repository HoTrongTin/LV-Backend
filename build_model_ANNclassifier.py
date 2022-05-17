import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# import missingno as mno
# from tqdm import tqdm
# from sklearn.base import BaseEstimator, TransformerMixin
# from sklearn.utils.validation import check_is_fitted
from tensorflow.keras import Sequential #framework
from tensorflow.keras.layers import Dense 
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
from sparkSetup import spark
# from collections import Counter
# import tensorflow as tf
# import numpy as np
import warnings
warnings.filterwarnings('ignore')

def build_model():
    #get data to train
    train_ = pd.read_csv('dataML/dataTrain/dataTrain0405.csv')
    y = train_.iloc[:,-1]
    X = train_.iloc[:,:-1]
    
    #random to split train data to train and test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=1, stratify=y)
    
    #RandomOverSampler
    over = RandomOverSampler(sampling_strategy=0.4)
    X_over, y_over = over.fit_resample(X_train, y_train)
    
    #RandomUnderSampler
    under = RandomUnderSampler(sampling_strategy=0.6)
    X_under, y_under = under.fit_resample(X_over, y_over)

    #Built ANN
    classifier = Sequential()
    classifier.add(Dense(16, input_dim=18, activation='relu'))
    classifier.add(Dense(8, activation='relu'))
    classifier.add(Dense(1, activation='sigmoid'))
    classifier.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    classifier.summary()

    #training model
    classifier.fit(X_under, y_under, validation_data=(X_test, y_test), epochs=50)

    #save model
    classifier.save('model_classifier/ANN_classifier')