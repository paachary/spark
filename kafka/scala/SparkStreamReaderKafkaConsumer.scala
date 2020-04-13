package org.paachary.kafka.scala

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class SparkStreamReaderKafkaConsumer {

  def beginStreaming : Unit = {

    val logger = LoggerFactory.getLogger(SparkStreamReaderKafkaConsumer.getClass.getName)

    val constants = new Constants

    val spark = SparkSession.builder.
      appName("SparkStreamingKafkaConsumer").
      config("spark.master", "local").
      getOrCreate()

    val aggregateTravelCount = spark.readStream.format("kafka").
      option("kafka.bootstrap.servers", constants.bootStrapServers).
      option("startingOffsets", "earliest").
      option("subscribe","tweet_topic").
      load()

    aggregateTravelCount.
      writeStream.
      format("memory").
      queryName("tweet_output").
      start().awaitTermination()

    logger.info(aggregateTravelCount.take(100).mkString("\n"))

  }
}

object SparkStreamReaderKafkaConsumer extends App  {
  val sparkStreamReaderKafkaConsumer = new SparkStreamReaderKafkaConsumer
  sparkStreamReaderKafkaConsumer.beginStreaming
}

