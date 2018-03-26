users = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);

users_filter = FILTER users BY (gender == 'M') AND (occupation == 'lawyer');

users_order = ORDER users_filter BY age DESC;

user = LIMIT users_order 1;

result = FOREACH user GENERATE userId;

STORE result INTO 'Q4_output';