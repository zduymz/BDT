REGISTER piggybank-0.17.0.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movies = LOAD 'MovieDataSet/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);

movies_filter = FILTER movies BY (genres MATCHES '.*Adventure.*');

rates = LOAD 'MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);

rates_group = GROUP rates BY movieId;

rates_avg = FOREACH rates_group GENERATE group AS (movieId:int), AVG(rates.rating) AS (rating:double);

movies_rating = COGROUP movies_filter BY movieId INNER, rates_avg BY movieId;

movies_rating_sort = FOREACH movies_rating GENERATE group as movieId, FLATTEN(movies_filter.title) AS title:chararray, FLATTEN(movies_filter.genres) AS genres:chararray, FLATTEN(rates_avg.rating) AS avg_rating:double;

movies_rating_order = ORDER movies_rating_sort BY avg_rating DESC, movieId ASC;

movies_top_20 = limit movies_rating_order 20;

result = FOREACH movies_top_20 GENERATE movieId, genres, avg_rating, title;


STORE result INTO 'Q6_output';
