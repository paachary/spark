/*
    Author : Prashant Acharya
    Description: Display only the 5 star rated movies using JOIN condition between 2 Rdds
    Date: 29/August/2019
 */
import org.apache.spark.SparkContext

object Popular5StarRatedMovies {

  def extractRatingFields( line : String) : ( Int, Int) = {
    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt
     (movieId, rating)
    }

  def extractMoviesFields( line : String) : ( Int, String) = {
    val fields = line.split("\\|")
    val movieId = fields(0).toInt
    val name:String = fields(1)
    (movieId, name)
  }

  def main(args : Array[String]) : Unit = {
    val sc = new SparkContext()

    val ratingsFile = sc.textFile("/home/hadoop/hadoop-materials/ml-100k/u.data")
    val moviesFile = sc.textFile("/home/hadoop/hadoop-materials/ml-100k/u.item")

    val moviesRdd = moviesFile.map(extractMoviesFields)

    val ratingsRdd = ratingsFile.map(extractRatingFields).filter(x => x._2 == 5).
      map( x => (x._1,1)).
      reduceByKey( (x,y) => (x+y))

    // Extract only the elements of the tupple using the map function below
    val joinRdd = ratingsRdd.join(moviesRdd).
      map(x => (x._2._1,x._2._2)).sortByKey(false).collect()

    for ( result <- joinRdd) {
      val count = result._1
      val movieName = result._2
      println(movieName+":"+ count)
    }
  }
}
