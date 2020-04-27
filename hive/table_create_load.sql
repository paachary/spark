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
