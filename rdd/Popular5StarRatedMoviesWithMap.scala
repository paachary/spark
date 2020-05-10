import java.nio.charset.CodingErrorAction

import Utilities._
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object Popular5StarRatedMoviesWithMap {

  def extractRatingFields( line : String) : ( Int, Int) = {
    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt
    (movieId, rating)
  }

  def loadMovies () : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames : Map[Int, String] = Map()

    val lines = Source.fromFile("/home/hadoop/hadoop-materials/ml-100k/u.item").getLines()

    for (line <- lines){

      var fields = line.split('|')

      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  def main ( args : Array[String]) : Unit = {

    val sc = new SparkContext()

    val moviesMap = loadMovies()

    val ratingsFile = sc.textFile("/home/hadoop/hadoop-materials/ml-100k/u.data")

    val ratingsRdd = ratingsFile.map(extractRatingFields).filter(x => x._2 == 5).
      map( x => (x._1,1)).
      reduceByKey( (x,y) => (x+y)).
      map(x=> (x._2, x._1)).
      sortByKey(false)

    for ( record <- ratingsRdd) {
      val movieId = record._2

      val ratingsCount = record._1

      println(moviesMap(movieId) + " : " + ratingsCount)
    }
  }
}
