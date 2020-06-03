 val output = spark.read.parquet("s3://prax-bucket/output")

 
 import org.apache.spark.sql.SaveMode

 output.write.format("jdbc").
 option("driver","com.amazon.redshift.jdbc42.Driver").
 option("url","<<jdbc url>>").
 option("ConnSchema","dev").
 option("dbtable","movies").
 option("user","awsuser").
 option("password","<<>>").
 option("dbtable","movies").
 option("truncate","false").
 mode(saveMode = SaveMode.Overwrite).
 save()
