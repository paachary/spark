create table if not exists 
movies (movie_id int, title String, genre String) 
comment 'table holding movies data' 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' ;

load data inpath 'data/movies.csv' overwrite into table movies;

