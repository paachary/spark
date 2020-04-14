package org.paachary.kafka.scala

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

case class FlightDetails(country : String, count: String)

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
      load().
      selectExpr("CAST(key as STRING) as country", "CAST(value as String) as count")

    import spark.implicits._
    val flightDetails = aggregateTravelCount.as[FlightDetails]

    //aggregateTravelCount.
    /*
    flightDetails.writeStream.
      format("console").
      queryName("tweet_output").
      trigger(Trigger.Once).
      start().awaitTermination()
      */

     // This is for writing the stream into the elasticsearch sink

     flightDetails.
     writeStream.
     //trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS)).
     format("es").
     option("checkpointLocation", "/tmp/checkpointLocation").
     option("es.mapping.id", "country").start("flight/country").awaitTermination()
  }
}

object SparkStreamReaderKafkaConsumer extends App  {
  val sparkStreamReaderKafkaConsumer = new SparkStreamReaderKafkaConsumer
  sparkStreamReaderKafkaConsumer.beginStreaming
}

