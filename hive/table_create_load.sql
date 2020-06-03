-- Managed Table
create table if not exists 
movies (movie_id int, 
	title String, 
	genre String) 
comment 'managed table holding movies data' 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' ;

load data inpath 'data/movies.csv' overwrite into table movies;


-- External Table
create external table if not exists
ratings (
	user_id int, 
	movie_id int,
	rating float,
	time_stamp int )
comment 'external table holding ratings data' 
row format delimited 
fields terminated by ',' lines 
terminated by '\n' 
location 'data' ;

load data inpath 'data/ratings.csv' into table ratings;

create external table movie_lens (movie_id int, occupation string, genre string, avg_rating string)
stored as parquet location 's3://workflow-1-bucket-2020/output/';


CREATE DATABASE s3movielens LOCATION 's3://prax-bucket/output/';

create external table s3movielens.movie_lens (movie_id int)
partitioned by (avg_rating DECIMAL(5,2), occupation string, genre string) 
stored as parquet location 's3://prax-bucket/output/';

MSCK REPAIR TABLE s3movielens.movie_lens;


select * from s3movielens.movie_lens_1;

