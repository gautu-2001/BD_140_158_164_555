from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType,StringType,DoubleType,IntegerType
import ast
from collections import OrderedDict
from pyspark.sql import Row
import sys
import json
import string
import nltk
from nltk.stem.porter import *
schema = StructType([
    StructField("sentiment",StringType(),True), #StringType is a datatype
    StructField("tweets",IntegerType(),True)
])
def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))






	
spark = SparkContext.getOrCreate()
ssc = StreamingContext(spark,2)
sqlContext = SQLContext(spark)
lines = ssc.socketTextStream("localhost",6100)
words = lines.flatMap(lambda line: line.split("\n"))

def converter(rdd):
	global schema
	rdd = rdd.collect()
	if len(rdd)==0:
		return
	dicts=rdd[0]
	#print(type(dicts),"-----------------------------------")
	k=ast.literal_eval(dicts)
	val=list(k.values())
	print(val)
	df = sqlContext.createDataFrame(data = k, schema = schema)
	df.printSchema()
	df.show(truncate=False)
words.foreachRDD(converter)

ssc.start() 
ssc.awaitTermination()
#preprocessing of data
def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
        
    return input_txt 


print('\n\nRemoving  Twitter Handles \n\n')
df['tidy_tweet'] = np.vectorize(remove_pattern)(df['tweet'], "@[\w]*")

print(df['tidy_tweet'].head())


print('\n\nRemoving Short Words\n\n')

df['tidy_tweet'] = df['tidy_tweet'].apply(lambda x: ' '.join([w for w in x.split() if len(w)>3]))

print(df['tidy_tweet'].head())

print('\n\nTweet Tokenization\n\n')
tokenized_tweet = df['tidy_tweet'].apply(lambda x: x.split())
print(tokenized_tweet.head())


print('\n\nStemming\n\n')

stemmer = PorterStemmer()

tokenized_tweet = tokenized_tweet.apply(lambda x: [stemmer.stem(i) for i in x])
 # stemming
print(tokenized_tweet.head())
 
#building model and accuracy


