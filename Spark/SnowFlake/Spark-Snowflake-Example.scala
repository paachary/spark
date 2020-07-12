
// Create an EMR cluster with EMR 5.30, with Spark application.
// It should support Spark 2.4.5 and Scala 2.11.12
// Download snowflake-jdbc-3.12.8.jar (or refer to the corresponding package from the maven repository)
// Download spark-snowflake_2.11-2.8.0-spark_2.4.jar (or refer to the corresponding package from the maven repository)

// Spark command-line from EMR 5.30, supporting Spark 2.4.5 and Scala 2.11.12
// spark-shell --jars snowflake-jdbc-3.12.8.jar,spark-snowflake_2.11-2.8.0-spark_2.4.jar

// Spark Program

val SNOWFLAKE_SOURCE_NAME="net.snowflake.spark.snowflake"

sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "<Fill it in>")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "<Fill it in>")


val defaultOptions = Map (
    "sfURL" -> "<<acccount>>.<<region>>.<<provider>>.snowflakecomputing.com",
    "sfUser" -> "<<username>>",
    "sfPassword" -> "<<password",
    "sfDatabase" -> "<<database>>",
    "sfSchema" -> "<<schema>>",
    "awsAccessKey" -> sc.hadoopConfiguration.get("fs.s3n.awsAccessKeyId"),
    "awsSecretKey" -> sc.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey")
)

def snowflakedf(sql: String) = {
    spark.read.format(SNOWFLAKE_SOURCE_NAME)
    .options(defaultOptions)
    .option("sfWarehouse", "COMPUTE_WH")
    .option("query", sql)
    .load()
}

// Reading a bunch of Parquet files already uploaded by an earlier Spark Job on the same EMR cluster
// The parquet files are partitioned by multiple attributes
val moviesList = spark.read.format("parquet").load("s3://<bucket-name>/topratedmovies")

import org.apache.spark.sql.SaveMode

// Save the above movieList dataframe into the snowflake table (already created from the snowflake GUI)
moviesList.write.
    format(SNOWFLAKE_SOURCE_NAME).
    options(defaultOptions).
    option("dbtable", "movies").
    option("sfWarehouse", "ANALYTICS_WH").
    mode(SaveMode.Overwrite).
    save()

// Now, query the table from snowflake to ensure the loading has been successful.
val df2 = snowflakedf("SELECT * FROM movies where avg_rating between 4 and 4.5")
df2.show(20,false)

// Trying with different queries.
val df2 = snowflakedf("SELECT * FROM movies where avg_rating between 4 and 4.5 order by avg_rating desc")
df2.show(20,false)


// Refer to the documentation for detailed use of parameters here:
// https://docs.snowflake.com/en/user-guide/spark-connector-use.html#moving-data-from-snowflake-to-spark

