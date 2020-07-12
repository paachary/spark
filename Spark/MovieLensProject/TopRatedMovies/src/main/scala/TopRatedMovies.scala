import org.apache.spark.sql.functions.{avg, col, count, expr, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TopRatedMovies{
  def getRatingsDataFrame(spark : SparkSession, inputFile : String) : DataFrame = {
    import spark.implicits._

    spark.read.format("text").load(inputFile).
      map{record =>
        val fields =
          record(0).toString.split("\\t")
        (fields(0).toInt,fields(1)toInt, fields(2).toDouble)
      }.
      withColumnRenamed("_1", "user_id").
      withColumnRenamed("_2","movie_id").
      withColumnRenamed("_3", "rating")
  }

  def getOnlyRatedMoviesGT10(spark : SparkSession, ratingsDataFrame : DataFrame) : DataFrame = {
    import spark.implicits._
    val movieWithRatingsGT10 = ratingsDataFrame.groupBy("movie_id").
      agg(count("movie_id").alias("movie_count")).
      filter("movie_count >= 10")

    val joinCondition = movieWithRatingsGT10("movie_id") === ratingsDataFrame("movie_id")
    val joinType = "inner"

    ratingsDataFrame.join(movieWithRatingsGT10, joinCondition, joinType).
      drop(movieWithRatingsGT10("movie_id")).drop(movieWithRatingsGT10("movie_count"))
  }

  def getUsersDataFrame(spark :SparkSession, fileName: String ) : DataFrame = {
    import spark.implicits._
    spark.read.format("text").load(fileName).
      map{record =>

        val fields = record(0).toString.split("\\|")
        (fields(0).toInt,fields(1)toInt, fields(2),fields(3),fields(4))
      }.
      withColumnRenamed("_1", "user_id").
      withColumnRenamed("_2","age").
      withColumnRenamed("_3", "gender").
      withColumnRenamed("_4", "occupation").
      withColumnRenamed("_5","zip_code")
  }

  def getMoviesDataFrame(spark :SparkSession, fileName: String ) : DataFrame = {
    import spark.implicits._
    spark.read.format("text").load(fileName).
      map{record =>
        val fields =
          record(0).toString.split("\\|")
        (fields(0).toInt,fields(1), fields(2))
      }.
      withColumnRenamed("_1", "movie_id").
      withColumnRenamed("_2","title").
      withColumnRenamed("_3", "release_dt")
  }

  def getMoviesGenreDataFrame(spark :SparkSession, fileName: String ) : DataFrame = {
    import spark.implicits._
    //"/home/hadoop/dataset/ml-100k/data/u.item"
    spark.read.format("text").load(fileName).
      map { record =>
        val fields =
          record(0).toString.split("\\|")
        (fields(0).toInt, fields(5), fields(6), fields(7),
          fields(8), fields(9), fields(10), fields(11), fields(12), fields(13),
          fields(14), fields(15), fields(16), fields(17), fields(18), fields(19),
          fields(20), fields(21), fields(22), fields(23)
        )
      }.
      withColumnRenamed("_1", "movie_id").
      withColumnRenamed("_2", "UNKNOWN").
      withColumnRenamed("_3", "ACTION").
      withColumnRenamed("_4", "ADVENTURE").
      withColumnRenamed("_5", "ANIMATION").
      withColumnRenamed("_6", "CHILDREN").
      withColumnRenamed("_7", "COMEDY").
      withColumnRenamed("_8", "CRIME").
      withColumnRenamed("_9", "DOCUMENTARY").
      withColumnRenamed("_10", "DRAMA").
      withColumnRenamed("_11", "FANTASY").
      withColumnRenamed("_12", "FILM_NOIR").
      withColumnRenamed("_13", "HORROR").
      withColumnRenamed("_14", "MUSICAL").
      withColumnRenamed("_15", "MYSTERY").
      withColumnRenamed("_16", "ROMANCE").
      withColumnRenamed("_17", "SCI_FI").
      withColumnRenamed("_18", "THRILLER").
      withColumnRenamed("_19", "WAR").
      withColumnRenamed("_20", "WESTERN").
      select($"movie_id"
        , expr("stack(19,'UNKNOWN',unknown," +
          " 'ACTION',action," +
          " 'ADVENTURE', ADVENTURE," +
          " 'CHILDREN', CHILDREN," +
          " 'COMEDY', COMEDY," +
          " 'CRIME', CRIME," +
          " 'DOCUMENTARY', DOCUMENTARY," +
          " 'DRAMA', DRAMA," +
          " 'ANIMATION', ANIMATION," +
          " 'FANTASY', FANTASY," +
          " 'FILM_NOIR', FILM_NOIR," +
          " 'HORROR', HORROR," +
          " 'MUSICAL', MUSICAL," +
          " 'MYSTERY', MYSTERY," +
          " 'ROMANCE', ROMANCE," +
          " 'SCI_FI', SCI_FI," +
          " 'THRILLER', THRILLER," +
          " 'WAR', WAR," +
          " 'WESTERN', WESTERN " +
          ") as (genre, total) ")
      ).filter("total != 0").drop("total")
  }

  def getMovieRatingsDataFrame(spark : SparkSession,
                               ratingsDataFrame         : DataFrame,
                               usersDataFrame           : DataFrame,
                               moviesDataFrame          : DataFrame,
                               moviesWithGenreDataFrame : DataFrame
                              )
  : DataFrame = {

    import spark.implicits._

    val joinCondition = ratingsDataFrame.repartition(5,$"movie_id")("movie_id") ===
      moviesDataFrame.repartition(5,$"movie_id")("movie_id")

    val joinType = "inner"

    val movieRatingsDF = moviesDataFrame.join(ratingsDataFrame,
      joinCondition, joinType).
      drop(ratingsDataFrame("movie_id"))

    val usersMoviesRatings = usersDataFrame.join(movieRatingsDF,
      usersDataFrame("user_id") ===   movieRatingsDF ("user_id"), "inner" ).
      drop(movieRatingsDF("user_id"))

    val userMovieGenreRatings = moviesWithGenreDataFrame.join(usersMoviesRatings,
      moviesWithGenreDataFrame("movie_id") ===  usersMoviesRatings("movie_id"), "inner").
      drop(moviesWithGenreDataFrame("movie_id"))

    userMovieGenreRatings.drop(userMovieGenreRatings("user_id")).
      groupBy("movie_id","title","occupation","genre").
      agg(round(avg("rating"),2).alias("avg_rating"))
  }
}


object TopRatedMovies {

  def main(args : Array[String]) : Unit = {

    if (args.length != 2){
      println("Insufficient arguments passed to the program:")
      println("\t Usage :")
      println("\t TopRatedMovies <input file directory> <output directory>\n\n")
    } else {

      val inputFileDir = args(0)
      val outputDir = args(1)

      val spark = SparkSession.builder().appName("Top Rated Movies").getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val topRatedMovies = new TopRatedMovies

      val ratingsDF = topRatedMovies.getRatingsDataFrame(spark, inputFileDir+"/u.data")

      ratingsDF.repartition(5,col("rating"))

      val filterRatedMovies = topRatedMovies.getOnlyRatedMoviesGT10(spark, ratingsDF)

      val moviesDF = topRatedMovies.getMoviesDataFrame(spark, inputFileDir+"/u.item")

      val usersDF =  topRatedMovies.getUsersDataFrame(spark, inputFileDir+"/u.user")

      val moviesWithGenreDF = topRatedMovies.getMoviesGenreDataFrame(spark, inputFileDir+"/u.item")

      val topRatedMoviesDF = topRatedMovies.getMovieRatingsDataFrame(spark,
        filterRatedMovies, usersDF, moviesDF , moviesWithGenreDF)

      val columns = Seq("avg_rating","occupation","genre")

      topRatedMoviesDF.write.partitionBy(columns:_*).
        format("parquet").mode("OVERWRITE").save(outputDir+"/topratedmovies")

      //val topMovieRatingsDF = spark.read.format("parquet").load(outputDir+"/output")

      //println(topMovieRatingsDF.count())

      spark.stop()
    }
  }
}
