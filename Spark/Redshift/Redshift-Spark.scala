 val output = spark.read.parquet("s3://prax-bucket/output")

 
 import org.apache.spark.sql.SaveMode

 output.write.format("jdbc").
 option("driver","com.amazon.redshift.jdbc42.Driver").
 option("url","jdbc:redshift://redshift-cluster-1.cyrrz8bfvxa1.us-east-1.redshift.amazonaws.com:5439").
 option("ConnSchema","dev").
 option("dbtable","movies").
 option("user","awsuser").
 option("password","Prax4Vats").
 option("dbtable","movies").
 option("truncate","false").
 mode(saveMode = SaveMode.Overwrite).
 save()
