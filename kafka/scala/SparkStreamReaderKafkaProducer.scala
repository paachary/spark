package org.paachary.kafka.scala

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class SparkStreamReaderKafkaProducer {

  def beginStreaming : Unit = {

    val logger = LoggerFactory.getLogger(SparkStreamReaderKafkaProducer.getClass.getName)

    val constants = new Constants

    val spark = SparkSession.builder.
      appName("SparkStreamingKafkaProducer").
      config("spark.master", "local").
      getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val staticDataFrame = spark.read.option("multiline","true").
      json("/home/hadoop/Spark-The-Definitive-Guide/data/flight-data/json")

    val streamingDataFrame = spark.readStream.schema(staticDataFrame.schema).
      option("maxFilesPerTrigger", 1).
      json("/home/hadoop/Spark-The-Definitive-Guide/data/flight-data/json")

    val aggregateTravelCount = streamingDataFrame.
      selectExpr(" (Dest_country_name||'-'||origin_country_name) as KEY").
      groupBy("KEY").
      count()

    aggregateTravelCount.selectExpr("CAST(KEY AS STRING) as key",
      "CAST(count AS STRING) as value").
      writeStream.
      format("kafka").
      option("kafka.bootstrap.servers",constants.bootStrapServers).
      option("checkpointLocation", "/home/hadoop/checkpoint").
      option("topic","tweet_topic").
      outputMode("complete").
      start().
      awaitTermination()

  }
}

object SparkStreamReaderKafkaProducer extends App {
  val sparkStreamReaderKafkaProducer = new SparkStreamReaderKafkaProducer
  sparkStreamReaderKafkaProducer.beginStreaming
}
