from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.linalg import SparseVector
from pyspark.sql.functions import *
import numpy as np

def get_mapping(rdd, idx):
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

def extract_features_dt(record):
    cat_array = [record[5], record[9]]
    cat_vec = np.zeros(cat_len) 
    i = 0
    step = 0
    for field in cat_array:
        m = mappings[i]
        idx = m[field]
        cat_vec[i] = idx 
        i = i + 1
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

sc = SparkContext(appName = "DecisionTree")

path = "file:///home/cloudera/Documents/hw11/Week11_SparkML/car_noheader.csv"
raw_data = sc.textFile(path)

records = raw_data.map(lambda x: x.split(","))

records.cache()

mappings = [get_mapping(records, i) for i in [5, 9]] 
cat_len = len(mappings)
num_len = 4
total_len = num_len + cat_len

print "Feature vector length for categorical features: %d" % cat_len 
print "Feature vector length for numerical features: %d" % num_len 
print "Total feature vector length: %d" % total_len

data_dt = records.map(lambda r: LabeledPoint(extract_label(r), extract_features_dt(r)))

first_point_dt = data_dt.first()
print "Label: " + str(first_point_dt.label)
print "Decision Tree feature vector: " + str(first_point_dt.features)
print "Decision Tree feature vector length: " + str(len(first_point_dt.features))

trainingData, testingData = data_dt.randomSplit([.9,.1],seed = 42)

dt_model = DecisionTree.trainRegressor(trainingData,{})
preds = dt_model.predict(testingData.map(lambda p: p.features))
actual = testingData.map(lambda p: p.label)
true_vs_predicted_dt = actual.zip(preds)
print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
print "Decision Tree depth: " + str(dt_model.depth())
print "Decision Tree number of nodes: " + str(dt_model.numNodes())

mse_dt = true_vs_predicted_dt.map(lambda (t, p): squared_error(t, p)).mean()
mae_dt = true_vs_predicted_dt.map(lambda (t, p): abs_error(t, p)).mean()
rmsle_dt = np.sqrt(true_vs_predicted_dt.map(lambda (t, p): squared_log_error(t, p)).mean())
print "Decision Tree - Mean Squared Error: %2.4f" % mse_dt
print "Decision Tree - Mean Absolute Error: %2.4f" % mae_dt
print "Decision Tree - Root Mean Squared Log Error: %2.4f" % rmsle_dt




