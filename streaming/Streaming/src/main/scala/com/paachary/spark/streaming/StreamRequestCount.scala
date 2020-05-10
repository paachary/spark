package com.paachary.spark.streaming

import com.paachary.accesslogparser.AccessLogParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, to_timestamp, udf, window}


class StreamRequestCount(spark: SparkSession , logFileLocation : String ) {

  val accessLogDF: DataFrame = spark.read.option("header","false").
    text(logFileLocation)

  val staticSchema = accessLogDF.schema

  val accessLogStreamingDF: DataFrame = spark.
    readStream.
    schema(staticSchema).
    option("maxFilesPerHour", 1).
    option("header", false).
    text(logFileLocation)

  import spark.implicits._

  val toTimestamp = udf[String, String](UDF.parse_clf_time(_))

  val streamingDF: DataFrame = accessLogStreamingDF.
    map { record =>
      val logParser = new AccessLogParser
      val fields = logParser.parseRecord(record.toString)
      val remoteHost = fields.get.clientIPAddress
      val request = fields.get.request
      val timestamp = fields.get.dateTime.split(" ")(0).split("\\[")(1)
      val statusCode = fields.get.httpStatusCode
      (remoteHost, timestamp, request, statusCode) }.
    selectExpr("_1 AS REMOTE_USER_IP", "_2 AS REQUEST_DATE", "_3 AS REQUESTED_URL", "_4 AS STATUS_CODE ").
    select(to_timestamp(toTimestamp($"REQUEST_DATE")).alias("REQUESTED_DATE_TIME"),$"*").
    groupBy($"REQUESTED_URL", window($"REQUESTED_DATE_TIME", "1 day")).
    agg(count("REQUESTED_URL") as "REQUESTED_URL_COUNT")

  streamingDF.
    writeStream.
    format("console").
    queryName("request_per_minute").
    outputMode("complete").
    start().
    awaitTermination()

}
