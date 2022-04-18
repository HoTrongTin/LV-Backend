import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# import missingno as mno
# from tqdm import tqdm
# from sklearn.base import BaseEstimator, TransformerMixin
# from sklearn.utils.validation import check_is_fitted
from tensorflow.keras import Sequential #khung 
from tensorflow.keras.layers import Conv2D
from tensorflow.keras.layers import MaxPooling2D
from tensorflow.keras.layers import Flatten
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
    train_ = pd.read_csv('../data/diabetes.csv')
    y = train_.iloc[:,-1]
    X = train_.iloc[:,1:-1]
    
    #random to split train data to train and test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=1, stratify=y)
    
    #RandomOverSampler
    over = RandomOverSampler(sampling_strategy=0.4)
    X_over, y_over = over.fit_resample(X_train, y_train)
    
    #RandomUnderSampler
    under = RandomUnderSampler(sampling_strategy=0.6)
    X_under, y_under = under.fit_resample(X_over, y_over)

    #reshape train data to tain CNN
    numRows_train = X_under.shape[0]
    numRows_test = X_test.shape[0]
    X_under_train = X_under.to_numpy().reshape(numRows_train,1,171,1)
    X_test = X_test.to_numpy()
    X_test = X_test.reshape(numRows_test,1,171,1)

    #Built CNN

    #Part 1: Create a box
    #Initialising the CNN - create an empty box
    classifier= Sequential()
    #Step 1- Convolution:
    classifier.add(Conv2D(32,(1,3), input_shape=(1,171,1),activation='relu'))
    #Step 2- Pooling:
    classifier.add(MaxPooling2D(pool_size=(1,2)))
    #Add a second convolutional layer
    classifier.add(Conv2D(32,(1,3), activation='relu'))
    classifier.add(MaxPooling2D(pool_size=(1,2)))
    # Step 3: Flattening
    classifier.add(Flatten()) # ma tran lam phang 
    # Step 4: Fully connection
    classifier.add(Dense(units=128, activation='relu')) #hidden layers # align with input_shape
    classifier.add(Dense(units=1, activation='sigmoid')) # ANN
    # Compiling the CNN
    classifier.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    classifier.summary()

    #training model
    classifier.fit(X_under_train, y_under, validation_data=(X_test, y_test), epochs=15)

    #save model
    classifier.save('model_classifier/CNN_classifier')