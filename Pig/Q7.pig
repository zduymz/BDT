--  Out of these highest rated top 20 movies found in Q6, how many male programmers have watched these movies?

REGISTER piggybank-0.17.0.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movies = LOAD 'MovieDataSet/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);

movies_filter = FILTER movies BY (genres MATCHES '.*Adventure.*');

rates = LOAD 'MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);

users = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);

users_filter = FILTER users BY (gender == 'M') AND (occupation == 'programmer');

rates_group = GROUP rates BY movieId;

rates_avg = FOREACH rates_group GENERATE group AS (movieId:int), AVG(rates.rating) AS (rating:double), rates.userId AS userGroup;

movies_rating = COGROUP movies_filter BY movieId INNER, rates_avg BY movieId;

movies_rating_sort = FOREACH movies_rating GENERATE group as movieId, FLATTEN(movies_filter.title) AS title:chararray, FLATTEN(movies_filter.genres) AS genres:chararray, FLATTEN(rates_avg.rating) AS avg_rating:double, FLATTEN(rates_avg.userGroup) as userGroup;

movies_rating_order = ORDER movies_rating_sort BY avg_rating DESC, movieId ASC;

movies_top_20 = limit movies_rating_order 20;

movies_top_20_usergroup = FOREACH movies_top_20 GENERATE FLATTEN(userGroup) as userId;

movies_top_20_usergroup_join = JOIN movies_top_20_usergroup BY userId, users_filter BY userId;

movies_top_20_usergroup_join_group = GROUP movies_top_20_usergroup_join ALL;

result = FOREACH movies_top_20_usergroup_join_group GENERATE COUNT(movies_top_20_usergroup_join);

STORE result INTO 'Q7_output';



