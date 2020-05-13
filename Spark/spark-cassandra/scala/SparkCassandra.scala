import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.SyntaxError
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 */
class SparkCassandra {

  val logger: Logger = LoggerFactory.getLogger(SparkCassandra.getClass)

  /**
   *
   * @param session
   * @return
   */
  private def createKeySpaceIfExists(session: Session): Boolean = {
    val result = session.execute("create keyspace if not exists movieKeySpace with replication = " +
      "{ 'class' : 'SimpleStrategy', 'replication_factor':3}")
    result.wasApplied()
  }

  /**
   *
   * @param session
   * @return
   */
  private def createAvgRatingsTable(session: Session): Boolean = {
    try {
      val result = session.execute("create table if not exists " +
        "moviekeyspace.movies_avg_ratings (movie_id int primary key, " +
        " movie_title varchar," +
        "avg_rating double)")
      result.wasApplied()
    } catch {
      case ex: SyntaxError => logger.error("Error occurred -> " + ex)
        session.close()
        return false
      case ex: Exception => logger.error("Error occurred ->" + ex)
        session.close()
        return false
    }
  }

  /**
   *
   * @param sparkSession
   * @param session
   */
  private def populateAvgRatingsTable(sparkSession: SparkSession,
                                      session: Session): Unit = {

    logger.info("Reading the csv file from hadoop")

    sparkSession.
      read.
      format("csv").
      option("inferSchema", "true").
      option("header", "true").
      load("hdfs://localhost:9000/user/hadoop/data/ratings.csv").
      createOrReplaceTempView("movies_ratings")

    logger.info("View movies_ratings successfully created")

    sparkSession.
      read.
      format("csv").
      option("inferSchema", "true").
      option("header", "true").
      load("hdfs://localhost:9000/user/hadoop/data/movies.csv").
      createOrReplaceTempView("movies")

    logger.info("View movies successfully created")

    sparkSession.sql("" +
      "SELECT movieid " +
      "FROM movies_ratings " +
      "GROUP BY movieid " +
      "HAVING count(1)> 10").createOrReplaceTempView("valid_movies")

    logger.info("View valid_movies successfully created")

    val resultAvgMovieRatingDF = sparkSession.sql("select v.movieid as movie_id, " +
      "m.title movie_title ,avg(r.rating) as avg_rating " +
      "from movies m INNER JOIN movies_ratings r " +
      " on (m.movieid = r.movieid)" +
      "INNER JOIN valid_movies v" +
      " on (r.movieid = v.movieid)" +
      "group by v.movieid, m.title order by avg(r.rating)")

    logger.info("Dataframe resultAvgMovieRatingDF successfully created")

    logger.info("Trying to populate the movies_avg_ratings in Cassandra")
    try {
      resultAvgMovieRatingDF.
        write.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "movies_avg_ratings",
          "keyspace" -> "moviekeyspace",
          "confirm.truncate" -> "true")).
        mode(saveMode = SaveMode.Overwrite).
        save()

      logger.info("Successfully populated the movies_avg_ratings in Cassandra")

      logger.info("Trying to verify the existance of Cassandra table after all the above operations")

      try {
        val createDDL =
          """ CREATE TEMPORARY VIEW movies_avg_ratings
        USING org.apache.spark.sql.cassandra
            |OPTIONS ( table "movies_avg_ratings",
            |keyspace "moviekeyspace",
            |pushdown "true")""".stripMargin

        sparkSession.sql(createDDL)
        val countDF = sparkSession.sql("select count(1) from movies_avg_ratings")
        logger.info("Table counts = " ++ countDF.collect().mkString(""))

        logger.info("Cassandra table has been sucessfully created and populated using Spark")
      } catch {
        case ex: Exception => logger.error("Error occurred while trying to fetch table from cassandra => " + ex)
          session.close()
          sparkSession.close()
      }

    } catch {
      case ex: Exception => logger.error("Exception -> " + ex)
        session.close()
        sparkSession.close()
    }
  }

  /**
   * Creating a cassandra session for executing the SQL stmts inside cassandra key space
   *
   * @param sparkSession - SparkSession
   * @return - DataStax Cassandra Session used for executing stmts
   */
  private def getCassandraConnection(sparkSession: SparkSession): Session = {
    val cassandraSession = CassandraConnector.apply(sparkSession.sparkContext.getConf)
    cassandraSession.openSession()
  }

  /**
   *
   */
  def init(): Unit = {
    val sparkSession = SparkSession.
      builder().
      master("local[*]").
      appName("SparkCassandra").
      config("spark.cassandra.connection.host", "localhost").
      config("spark.cassandra.connection.port", "9042").
      config("spark.cassandra.output.consistency.level", "LOCAL_ONE").
      config("spark.cassandra.input.consistency.level", "LOCAL_ONE").
      getOrCreate()

    logger.info("Trying to get the cassandra connection")

    val cassandraSession: Session = getCassandraConnection(sparkSession)

    logger.info("Got the cassandra connection")

    logger.info("Trying to create the keyspace if it doesnt exist")

    if (!createKeySpaceIfExists(cassandraSession))

      logger.error("Something went wrong... Please check with administrator")

    else {
      // create cassandra table
      logger.info("Trying to create table if it doesnt exist")

      if (!createAvgRatingsTable(cassandraSession)) {

        logger.error("Something went wrong... Please check with administrator")

      } else {

        logger.info("Populating the table")

        populateAvgRatingsTable(sparkSession, session = cassandraSession)

        logger.info("Processing complete")
      }
    }
    logger.info("Closing the cassendra session")

    cassandraSession.close()

    logger.info("Cassandra Session closed")

    sparkSession.close()

    logger.info("Spark session closed")
  }

}

object SparkCassandra extends App{

  val sparkCassandra = new SparkCassandra
  sparkCassandra.init()
}
