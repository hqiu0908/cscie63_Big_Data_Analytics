from pyspark import SparkContext
import pyspark.mllib
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import *
import numpy as np

def get_mapping(rdd, idx):
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

def extract_features(record):
    cat_array = [record[5], record[9]]
    cat_vec = np.zeros(cat_len) 
    i = 0
    step = 0
    for field in cat_array:
        m = mappings[i]
        idx = m[field] 
        cat_vec[idx + step] = 1 
        i= i + 1
        step = step + len(m)
    num_array = [record[2], record[3], record[7], record[10]]
    num_vec = np.array([float(field) for field in num_array])
    return np.concatenate((cat_vec, num_vec))
    
def extract_label(record): 
    # return float(record[1])
	return float(record[4])

def abs_error(actual, pred):
    return np.abs(pred - actual)

def squared_error(pred, actual):
    return (pred - actual)**2

def squared_log_error(pred, actual):
    return (np.log(pred + 1) - np.log(actual + 1))**2

sc = SparkContext(appName = "LinearRegression")

path = "file:///home/cloudera/Documents/hw11/Week11_SparkML/car_noheader.csv"
raw_data = sc.textFile(path)

records = raw_data.map(lambda x: x.split(","))

records.cache()

mappings = [get_mapping(records, i) for i in [5, 9]] 
cat_len = len(get_mapping(records, 5)) + len(get_mapping(records, 9))
num_len = 4
total_len = num_len + cat_len

print "Feature vector length for categorical features: %d" % cat_len 
print "Feature vector length for numerical features: %d" % num_len 
print "Total feature vector length: %d" % total_len

data = records.map(lambda r: LabeledPoint(extract_label(r),extract_features(r)))

first_point = data.first()
print "Label: " + str(first_point.label)
print "Linear Model feature vector:\n" + str(first_point.features) 
print "Linear Model feature vector length: " + str(len(first_point.features))

trainingData, testingData = data.randomSplit([.9,.1],seed = 42)

linear_model = LinearRegressionWithSGD.train(trainingData, iterations=100, step=0.000001)
true_vs_predicted = testingData.map(lambda p: (p.label, linear_model.predict(p.features)))
print "Linear Model predictions: " + str(true_vs_predicted.take(5))

mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean() 
rmsle = np.sqrt(true_vs_predicted.map(lambda(t,p):squared_log_error(t,p)).mean()) 
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f" % mae
print "Linear Model - Root Mean Squared Log Error: %2.4f" % rmsle

