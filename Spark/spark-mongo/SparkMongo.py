from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *


def get_spark_session():
    return (SparkSession \
        .builder \
        .appName("SparkMongoApp") \
        .config("spark.mongodb.input.uri",\
            "mongodb://127.0.0.1/movielens.avg_rating_by_occupation?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/movielens.avg_rating_by_occupation") \
        .config("spark.mongodb.input.uri",\
            "mongodb://127.0.0.1/movielens.avg_rating_by_movies?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/movielens.avg_rating_by_movies") \
        .getOrCreate())


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
        format("mongo").\
        option("database","movielens").\
        option("collection","avg_rating_by_movies").\
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
        format("mongo").\
        option("database","movielens").\
        option("collection","avg_rating_by_occupation").\
        mode("overwrite").\
        save()


def read_get_avg_rating_by_movies_from_db(spark):
    return (spark.read.format("mongo").\
    option("database","movielens").\
    option("collection","avg_rating_by_movies").\
    load())



def read_avg_rating_by_occupation_from_db(spark):
    return (spark.read.format("mongo").\
    option("database","movielens").\
    option("collection","avg_rating_by_occupation").\
    load())



def init():
    spark = get_spark_session()
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

        ## Writing into the mongodb database
        prepare_avg_rating_by_movies_dataset(avg_ratings_df, moviesDF)
        prepare_avg_rating_by_occupation(avg_user_ratings_df, usersDF)
        
        # Reading the "avg_rating_by_movies" inserted data from database 
        # and displaying on the console
        read_get_avg_rating_by_movies_from_db(spark).drop("_id").sort(desc("avg_rating")).show()

        # Reading the "avg_rating_by_occupation" inserted data from database 
        # and displaying on the console
        read_avg_rating_by_occupation_from_db(spark).drop("_id").sort(desc("rating_count")).show()

        # Stopping the spark session
        spark.stop()
    
if __name__ == "__main__":
    init()
    
