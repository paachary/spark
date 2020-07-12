import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

def getRatingsDataFrame(spark : SparkSession, inputFile : String) : DataFrame = {
    import spark.implicits._
    //"/home/hadoop/dataset/ml-100k/data/u.data"
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
    //"/home/hadoop/dataset/ml-100k/data/u.user"
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
    //"/home/hadoop/dataset/ml-100k/data/u.item"
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

  def getUserPreferencesDataFrame(
                                moviesDataFrame               : DataFrame,
                                movieGenreDataFrame           : DataFrame,
                                usersDataFrame                : DataFrame,
                                filteredMovieRatingsDataframe : DataFrame
                                ) : DataFrame = {            
    val join_condition_1 = moviesDataFrame.repartition(5,$"movie_id")("movie_id") === 
      movieGenreDataFrame.repartition(5,$"movie_id")("movie_id")
    val join_type_1 = "inner"

    val completeMovieDF = moviesDataFrame.join(movieGenreDataFrame, join_condition_1, join_type_1).
      drop(movieGenreDataFrame("movie_id"))

    val join_condition_2 = completeMovieDF.repartition(5,$"movie_id")("movie_id") === 
      filteredMovieRatingsDataframe.repartition(5,$"movie_id")("movie_id") 
    val join_type_2= "inner"

    val fullMoviesListDF = completeMovieDF.join(filteredMovieRatingsDataframe, join_condition_2, join_type_2).
      drop(filteredMovieRatingsDataframe("movie_id"))

    val join_condition_3 = fullMoviesListDF.repartition(5,$"user_id")("user_id") === 
      usersDataFrame.repartition(5,$"user_id")("user_id")
    val join_type3 = "inner"

    usersDataFrame.join(fullMoviesListDF, join_condition_3, join_type3).
      drop(fullMoviesListDF("user_id"))
  }

val ratingsDF = getRatingsDataFrame(spark, "u.data")

ratingsDF.repartition(5,col("rating"))

val filterRatedMovies = getOnlyRatedMoviesGT10(spark, ratingsDF)

val usersDF =  getUsersDataFrame(spark, "u.user")

val moviesDF = getMoviesDataFrame(spark, "u.item")

val moviesGenreDF = getMoviesGenreDataFrame(spark, "u.item")

def getMovieRatingsDataFrame(spark : SparkSession, ratingsDataFrame : DataFrame, moviesDataFrame  : DataFrame) 
: DataFrame = {
    
    val joinCondition = ratingsDataFrame.repartition(5,$"movie_id")("movie_id") === 
    moviesDataFrame.repartition(5,$"movie_id")("movie_id")
    val joinType = "inner"
    
    val movieRatingsDF = moviesDataFrame.join(ratingsDataFrame, joinCondition, joinType).drop(ratingsDataFrame("movie_id"))
    
    val usersDF =  getUsersDataFrame(spark, "u.user")
    
    val usersMoviesRatings = usersDF.join(movieRatingsDF,
               usersDF("user_id") ===   movieRatingsDF ("user_id"), "inner" ).
    drop(movieRatingsDF("user_id"))
   
    val moviesWithGenreDF = getMoviesGenreDataFrame(spark, "u.item")
    
    val userMovieGenreRatings = moviesWithGenreDF.join(usersMoviesRatings,
               moviesWithGenreDF("movie_id") ===  usersMoviesRatings("movie_id"), "inner").
    drop(moviesWithGenreDF("movie_id"))
    
    userMovieGenreRatings.drop(userMovieGenreRatings("user_id")).groupBy("movie_id","occupation","genre").
    agg(round(avg("rating"),2).alias("avg_rating"))   

}

val topRatedMoviesDF = getMovieRatingsDataFrame(spark, filterRatedMovies, moviesDF )

topRatedMoviesDF.filter("avg_rating = 4.15").groupBy("occupation","genre").agg(count("movie_id")).show(100,false)

val columns = Seq("avg_rating","occupation","genre")
topRatedMoviesDF.write.partitionBy(columns:_*).format("parquet").mode("OVERWRITE").save("output") 

val topMovieRatingsDF = spark.read.format("parquet").load("output/")

topMovieRatingsDF.agg(max("avg_rating")).explain

topMovieRatingsDF.filter("avg_rating = 4.05").groupBy("genre").agg(count("avg_rating").alias("movie_genre_count")).show(100,false)

topMovieRatingsDF.filter("avg_rating = 4.05").explain

topMovieRatingsDF.filter("avg_rating = 4.05").show(100,false)


