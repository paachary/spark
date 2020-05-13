package com.paachary.spark.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamAccessLog extends App{

  if (args.length != 2){
    println("Wrong number of arguments passed to the program..\n\n\t Usage>>\n" +
      "\t\tStreamAccessLog \"<log file absolute path>\"  \"<type of report : request / status>\"\n\n")
  } else {
    val logFilePath = args(0)
    val reportType = args(1)

    val spark = SparkSession.
      builder.
      master("local[*]").
      appName("StreamingAccessLog").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if (reportType.equals("request"))
      new StreamRequestCount(spark, logFilePath)
    else if (reportType.equals("status"))
      new StreamStatusCount(spark, logFilePath)
    else
      println("Wrong value of report type passed to the program. - request / status\n")
  }
}
