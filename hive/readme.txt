# Starting hive metastore service on a particular node
hive --service metastore -p 9083 --hiveconf hive.root.logger=INFO,console

# starting hive in the debug mode
hive -hiveconf hive.root.logger=DEBUG,console 
