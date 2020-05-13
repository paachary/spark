package com.paachary.spark.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object Streaming  extends  App{

  val spark = SparkSession.builder().appName("SparkStreamingWindowExample").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val staticDataFrame: DataFrame = spark.read.format("csv").option("inferSchema", "true").option("header", "true").
    csv("/home/hadoop/Spark-The-Definitive-Guide/data/retail-data/by-day/*")

  val schema: StructType = staticDataFrame.schema

  val streamingDataFrame: DataFrame = spark.
    readStream.
    schema(schema).
    option("maxFilesPerTrigger",1).
    option("header","true").
    csv("/home/hadoop/Spark-The-Definitive-Guide/data/retail-data/by-day/*")

  val purchaseByCustomerPerHour: DataFrame = streamingDataFrame.
    selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate" ).
    groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).
    sum("total_cost")

  purchaseByCustomerPerHour.writeStream.format("console").
    queryName("customer_purchases_by_day").
    outputMode("complete").
    start().
    awaitTermination()
}
