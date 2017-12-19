# Databricks notebook source
import pandas as pd
import numpy as np


# COMMAND ----------

# Convenience function for plotting
def show_plot(sequence):
  display(pd.DataFrame(sequence).plot(kind='line').figure)

# COMMAND ----------

df = pd.read_csv("https://raw.githubusercontent.com/jaungiers/LSTM-Neural-Network-for-Time-Series-Prediction/master/sinwave.csv")

# COMMAND ----------

df.head(20)

# COMMAND ----------

show_plot(df)

# COMMAND ----------

df.shape

# COMMAND ----------

show_plot(df[0:10])

# COMMAND ----------

# MAGIC %md 
# MAGIC We want to take our series and transform it into a tensor of the dimensions `(number_windows, window_size, num_features)`.
# MAGIC 
# MAGIC Since we are only dealing with a single feature (a single time series signal), `num_features = 1`.

# COMMAND ----------

lst = np.arange(0,20)
print(lst)

# COMMAND ----------

###********* num_windows calculation explanation ****************###
# Indexer will be a matrix of indices. The bottom right corner index is the last index of your data set. The last index of your data set must be equal to the length of your data set. 
# Each column is a multiple of `overlap`. Hence, if you choose `overlap = 2`, then your columns will be something like this:
#
# array([[ 0,  1,  2,  3,  4],
#        [ 2,  3,  4,  5,  6],
#        [ 4,  5,  6,  7,  8],
#        [ 6,  7,  8,  9, 10],
#        [ 8,  9, 10, 11, 12],
#        [10, 11, 12, 13, 14],
#        [12, 13, 14, 15, 16],
#        [14, 15, 16, 17, 18]])
#
# As you can see, each column is a multiple of `overlap`. Hence, in order to ensure the last index is less than the `total_length` of your dataset, you need to ensure that 

# `(window_size - 1) + num_windows*overlap < total_length`

# (Note that I use `window_size - 1` because python starts indices at zero.)

# Solving for num_windows gives us 

# `num_windows = (total_length - (window_size - 1))//overlap `

# where `//` is floor divide.

# Also for an explanation of why adding a row array to a column array in numpy produces a 2-d array, read this: http://www.scipy-lectures.org/intro/numpy/operations.html#broadcasting
###**************************************************###

def create_windows(sequence, window_size, overlap):
  
  sequence = np.ravel(sequence)
  
  assert len(sequence.shape) == 1, "sequence must be a 1-d array"
  assert isinstance(sequence, np.ndarray), "sequence must be of type np.ndarray" #TODO: Consider removing to support duck-typing.

  assert overlap < window_size, "overlap must be less than window_size"
  assert window_size > 0, "window_size must be greater than zero"
  assert window_size < len(sequence), "window_size must be less than the total length of sequence"
  
  total_length = len(sequence)
  offset = window_size - overlap
  
  num_windows = (total_length - (window_size - 1))//offset # WARNING: This will only generate complete windows. If there are not enough points at the end to fill a window, the points are dropped.
  indexer = np.arange(window_size)[None, :] + offset*np.arange(num_windows)[:, None]

  assert indexer[-1][-1] < total_length #ensure that the last index in the array is less than the total length.

  return sequence[indexer]


# COMMAND ----------

lst = np.arange(0,20)
print(lst)

# COMMAND ----------

create_windows(lst, 5, 1)

# COMMAND ----------

# MAGIC %md ## Transform sine wave into windows

# COMMAND ----------

window_size = 50
overlap=49

windowed_df = create_windows(df, window_size=window_size, overlap=overlap)
windowed_df

# COMMAND ----------

show_plot(windowed_df[0])

# COMMAND ----------

show_plot(windowed_df[1])

# COMMAND ----------

# MAGIC %md ### Separate test and train

# COMMAND ----------

train_percentage = .7 # Train on the first 90% of the signal

train = windowed_df[:int(train_percentage*windowed_df.shape[0])]
test = windowed_df[int(train_percentage*windowed_df.shape[0]):]

# COMMAND ----------

print(windowed_df.shape)
print(train.shape)
print(test.shape)

assert train.shape[0] + test.shape[0] == windowed_df.shape[0]


# COMMAND ----------

X_train = train[:,:-1]
X_test = test[:,:-1]
y_train = train[:,-1]
y_test = test[:,-1]

# COMMAND ----------

show_plot(train[1])

# COMMAND ----------

# Keras wants input of the shape (number_windows, window_size, num_features).

X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))

# COMMAND ----------

show_plot(y_train)

# COMMAND ----------

# MAGIC %md ### Create model

# COMMAND ----------

import keras
keras.__version__

# COMMAND ----------

from keras.models import Sequential
from keras.layers import Dense, Activation, LSTM, Dropout
import time

def build_model(layers):
    model = Sequential()

    model.add(LSTM(
        input_dim=layers[0],
        output_dim=layers[1],
        return_sequences=True))
    model.add(Dropout(0.2))

    model.add(LSTM(
        layers[2],
        return_sequences=False))
    model.add(Dropout(0.2))

    model.add(Dense(
        output_dim=layers[3]))
    model.add(Activation("linear"))

    start = time.time()
    model.compile(loss="mse", optimizer="rmsprop")
    print("> Compilation Time : ", time.time() - start)
    return model
  


# COMMAND ----------

epochs  = 1
seq_len = 50

print('> Loading data... ')

#X_train, y_train, X_test, y_test = lstm.load_data('sp500.csv', seq_len, True)

print('> Data Loaded. Compiling...')

model = build_model([1, 50, 100, 1])

model.fit(
    X_train,
    y_train,
    batch_size=512,
    nb_epoch=epochs,
    validation_split=0.05)



# COMMAND ----------

def predict_point_by_point(model, data):
    #Predict each timestep given the last sequence of true data, in effect only predicting 1 step ahead each time
    predicted = model.predict(data)
    predicted = np.reshape(predicted, (predicted.size,))
    return predicted

predicted = predict_point_by_point(model, X_test)


# COMMAND ----------

res = pd.DataFrame(data={"predicted": predicted, "actual": y_test})
display(res.plot(kind='line').figure)


# COMMAND ----------

