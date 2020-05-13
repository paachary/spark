import cassandra
import pyspark
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *


def get_cassandra_connection(host_name):
    cluster  = Cluster([host_name])
    return cluster.connect()


def get_spark_session(host, port):
    return (SparkSession.\
                builder.\
                master("local[*]").\
                appName("SparkCassandra").\
                config("spark.cassandra.connection.host", host).\
                config("spark.cassandra.connection.port", port).\
                config("spark.cassandra.output.consistency.level", "LOCAL_ONE").\
                config("spark.cassandra.input.consistency.level", "LOCAL_ONE").\
                getOrCreate())


def create_keyspace(session, keyspace, replication_options = None):
    
    replication_options = replication_options or \
                          "{'class': 'SimpleStrategy', 'replication_factor': 3}"
    
    result = session.execute(""" 
            CREATE KEYSPACE IF NOT EXISTS {} 
            WITH REPLICATION = {} """.format(keyspace, replication_options))


def create_avg_rating_by_movies_table(session, keyspace):
    session.execute("""
                CREATE TABLE IF NOT EXISTS {}.avg_rating_by_occupation 
                    (occupation TEXT PRIMARY KEY, 
                     rating_count DOUBLE);
        """.format(keyspace))


def create_avg_rating_by_movies_table(session, keyspace):
    session.execute("""
            CREATE TABLE IF NOT EXISTS {}.avg_rating_by_movies 
                (movie_id INT PRIMARY KEY, 
                title TEXT, 
                release_dt TEXT,
                rating_count DOUBLE, 
                avg_rating DOUBLE );
        """.format(keyspace))

    
def get_ratings_df(spark):
    return (spark.sparkContext.\
        textFile("file:///home/hadoop/tutorial/HadoopMaterials/ml-100k/u.data").\
        map(lambda f : f.split("\t") ).\
        toDF().\
        selectExpr("cast(_1 as int) as user_id",\
        "cast(_2 as int) as movie_id",\
        "cast(_3 as double) as rating",\
        "cast(_4 as bigint) as timestamp"))


def get_users_df(spark):
    return (spark.sparkContext.\
        textFile("file:///home/hadoop/tutorial/HadoopMaterials/ml-100k/u.user").\
        map(lambda f : f.split("|") ).\
        toDF().\
        selectExpr("cast(_1 as int) as user_id",\
        "cast(_2 as int) as age",\
        "cast(_3 as string) as gender",\
        "cast(_4 as string) as occupation",\
        "cast(_5 as string) as zipcode"))


def get_movies_df(spark):    
    return (spark.sparkContext.\
        textFile("file:///home/hadoop/tutorial/HadoopMaterials/ml-100k/u.item").\
        map(lambda f : f.split("|") ).\
        toDF().\
        selectExpr("cast(_1 as int) as movie_id",\
        "cast(_2 as string) as title",\
        "cast(_3 as string) as release_dt"))


def get_avg_ratings_df(ratingsDF):
    return (ratingsDF.\
        selectExpr("movie_id as ratings_movie_id","rating").\
        groupBy("ratings_movie_id").\
        agg(count("rating").alias("rating_count"), \
        avg("rating").alias("avg_rating")).\
        filter("rating_count > 10"))
    

def get_avg_user_ratings_df(ratingsDF):
    return (ratingsDF.selectExpr("user_id as ratings_user_id", "rating").\
        groupBy("ratings_user_id", "rating").\
        agg(count("rating").alias("rating_count")).\
        filter("rating_count > 10").\
        drop("rating_count"))


def prepare_avg_rating_by_movies_dataset(avg_ratings_df, moviesDF):   
    join_expression = avg_ratings_df["ratings_movie_id"] == moviesDF["movie_id"]

    join_type = "inner"

    avg_rating_by_movies_dataset = \
    moviesDF.\
    join(avg_ratings_df,join_expression,join_type).\
    drop("ratings_movie_id").\
    sort(desc("avg_rating"))
    
    avg_rating_by_movies_dataset.\
    write.\
        format("org.apache.spark.sql.cassandra").\
        options(keyspace="movielens",\
                table="avg_rating_by_movies").\
        option("confirm.truncate","true").\
        mode("overwrite").\
        save()


def prepare_avg_rating_by_occupation(avg_user_ratings_df, usersDF):
    join_expression = avg_user_ratings_df["ratings_user_id"] == usersDF["user_id"]

    join_type = "inner"

    avg_rating_by_occupation_dataset = \
    usersDF.join(avg_user_ratings_df,join_expression,join_type).\
    drop("ratings_user_id","age","zipcode","user_id","gender").\
    groupBy("occupation").\
    agg(count("rating").alias("rating_count")).\
    sort(desc("rating_count"))
    
    avg_rating_by_occupation_dataset.\
    write.\
        format("org.apache.spark.sql.cassandra").\
        options(keyspace="movielens",\
                table="avg_rating_by_occupation").\
        option("confirm.truncate","true").\
        mode("overwrite").\
        save()


def read_get_avg_rating_by_movies_from_db(spark):
    return (spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="movielens",\
            table="avg_rating_by_movies").\
    load())



def read_avg_rating_by_occupation_from_db(spark):
    return (spark.read.format("org.apache.spark.sql.cassandra").\
    options(keyspace="movielens",\
            table="avg_rating_by_occupation").\
    load())


def init(host_name, port, keyspace):
    
    print("host = {}: port = {} : keyspace = {}".format(host_name, port, keyspace))

    spark = get_spark_session(host_name, port)
    if (spark == None):
        print("sparkSession is null")
        exit
    else:

        # Setting the logging level
        spark.sparkContext.setLogLevel("ERROR")
        
        # Reading from source files
        ratingsDF = get_ratings_df(spark)
        usersDF = get_users_df(spark)
        moviesDF = get_movies_df(spark)

        ## Prepating the data for inserting..
        avg_ratings_df = get_avg_ratings_df(ratingsDF)
        #avg_ratings_df.show()
        
        avg_user_ratings_df = get_avg_user_ratings_df(ratingsDF)
        #avg_user_ratings_df.show()
        
        # Prepare cassandra session.
        session = get_cassandra_connection(host_name)
        
        # Create the keyspace
        create_keyspace(session, keyspace)
        
        session.set_keyspace(keyspace)
                
        # Create the avg_rating_by_movies table in the keyspace
        create_avg_rating_by_movies_table(session, keyspace)
        
        # Create the avg_rating_by_movies table in the keyspace
        create_avg_rating_by_movies_table(session, keyspace)

        ## Writing into the mongodb database
        prepare_avg_rating_by_movies_dataset(avg_ratings_df, moviesDF)
        prepare_avg_rating_by_occupation(avg_user_ratings_df, usersDF)
        
        # Reading the "avg_rating_by_movies" inserted data from database 
        # and displaying on the console
        read_get_avg_rating_by_movies_from_db(spark).drop("_id").sort(desc("avg_rating")).show()

        # Reading the "avg_rating_by_occupation" inserted data from database 
        # and displaying on the console
        read_avg_rating_by_occupation_from_db(spark).drop("_id").sort(desc("rating_count")).show()
        
        # Closing the cassandra session
        session.shutdown()

        # Stopping the spark session
        spark.stop()

import sys

def main():
    
    print(len(sys.argv))

    if (len(sys.argv) != 4):
        print("Please pass the appropriate arguments to the program..\n")
        print("\t Usage >> \n")
        print("\t\t SparkCassandra <cassandra host name>> <<cassandra port>> <<cassandra keyspace>>\n\n")

    else:

        host_name = sys.argv[1].strip()
        port      = sys.argv[2].strip()
        keyspace  = sys.argv[3].strip()
        
        if host_name is None or host_name == "":
            host_name = "127.0.0.1"
        
        if port is None or port == "":
            port = "9042"
            
        if keyspace is None or keyspace == "":
            keyspace = "movielens"       
        
        init(host_name, 
             port,
             keyspace)


if __name__ == "__main__":
    main()
