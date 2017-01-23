from pyspark import SparkContext
import pyspark.mllib
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import *
import numpy as np

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_error(pred, actual):
    return (pred - actual)**2

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2

sc = SparkContext(appName = "LinearRegressionSimple")

path = "file:///home/cloudera/Documents/hw11/Week11_SparkML/car_noheader.csv"
raw_data = sc.textFile(path)

records = raw_data.map(lambda x: x.split(","))

data = records.map(lambda line:LabeledPoint(line[4], [line[3]]))

trainingData, testingData = data.randomSplit([.9,.1],seed=42)

linear_model = LinearRegressionWithSGD.train(trainingData, iterations=100, step=0.00001)
true_vs_predicted = testingData.map(lambda p: (p.label, linear_model.predict(p.features)))
print "Linear Model predictions: " + str(true_vs_predicted.take(9))

# linear_model.save(sc, "file:///home/cloudera/Documents/hw11/Week11_SparkML/linear_model")
true_vs_predicted.saveAsTextFile("file:///home/cloudera/Documents/hw11/Week11_SparkML/output")

mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean() 
rmsle = np.sqrt(true_vs_predicted.map(lambda(t,p):squared_log_error(t,p)).mean()) 
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f" % mae
print "Linear Model - Root Mean Squared Log Error: %2.4f" % rmsle

