%spark
// spark = spark session

// Ratings file
val ratingsFile = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/user/hadoop/data/ratings.csv")
ratingsFile.printSchema

//Movies file
val moviesFile = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/hadoop/data/movies.csv")
moviesFile.printSchema


// put the filter of count > 10 to avoid getting avg ratings of movies which have been rated by less than 10 people
%spark
val ratingsWithFilter = ratingsFile.
selectExpr("movieid as ratingMovieId", "rating").
groupBy("ratingMovieId").
agg(count("rating").alias("cnt"), avg("rating").alias("avg")).
where(" cnt > 10")


// Join operation between ratings and movies dataframes.
%spark

val joinExpresession = ratingsWithFilter.col("ratingMovieId") === moviesFile.col("movieid")

val joinType = "inner"

// apply the join and the filter condition
// This sql provides us with the final result

ratingsWithFilter.select("ratingMovieId","avg").
join(moviesFile.selectExpr("movieid", "title"), joinExpresession, joinType).
selectExpr("ratingMovieid","title","avg").
orderBy(desc("avg")).
show(20,false)


// Following sql gives the same results as the above operations using Dataframe APIs.

ratingsFile.createOrReplaceTempView("ratings")
moviesFile.createOrReplaceTempView("movies")

spark.sql("select r.movieid, m.title, "+
" avg(r.rating) "+
" from ratings r INNER JOIN movies m "+
" on m.movieid = r.movieid " +
" where m.movieid in (  select r1.movieid "+
                        "from ratings r1 "+
                        "group by r1.movieid having count(*) > 10)  "+
  "group by r.movieid, m.title "+
  " order by avg(r.rating) desc").collect()



