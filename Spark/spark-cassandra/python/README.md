
Install cassandra driver for cassandra:

pip install cassandra-driver

Executing the spark script using the spark-submit job:

spark-submit --packages anguenot/pyspark-cassandra:2.4.0  --conf spark.cassandra.connection.host=127.0.0.1 SparkCassandra.py <<<cassandrahostname>>> <<<cassandraport>>> <<<cassandrakeyspace>>>
