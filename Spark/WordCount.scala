import org.apache.spark.sql.SparkSession

object WordCount extends App {

  if (args.length != 2)
    println("Usage WordCount <inputfile> <delimiter>")
  else {
    val inputFile = args(0)
    val delimiter = args(1)
    val spark = SparkSession.builder().appName("WordCount").master("local[*]").
      getOrCreate()

    val result = spark.read.textFile(inputFile).
      selectExpr(s"explode(split(value, '$delimiter')) as value ").
      groupBy("value").
      count()

      result.write.csv("/home/hadoop/word_count_spark_output")
  }
}
