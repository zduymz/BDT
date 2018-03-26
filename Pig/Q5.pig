REGISTER piggybank-0.17.0.jar

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movies = LOAD 'MovieDataSet/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);

movies_filter = FILTER movies BY STARTSWITH(title, 'a') OR STARTSWITH(title, 'A');

movies_by_genres = FOREACH movies_filter GENERATE TOKENIZE(genres, '|');

movies_genres_flatten = FOREACH movies_by_genres GENERATE FLATTEN($0) AS genre;

movies_group = GROUP movies_genres_flatten BY genre;

result = FOREACH movies_group GENERATE group, COUNT(movies_genres_flatten.genre);

STORE result INTO 'Q5_output';

