install pyspark for running and testing spark code using python on pyspark shell

install pymongo for accessing mongo APIs inside python script

Use the mongoDB connector for spark : org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 for executing the 

pyspark code.

Executing the spark script using the spark-submit job:
------------------------------------------------------
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1 SparkMongo.py
