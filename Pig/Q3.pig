users = LOAD 'MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);

users_filter = FILTER users BY (gender == 'M') AND (occupation == 'lawyer');

users_group = GROUP users_filter BY (gender, occupation);

result = FOREACH users_group GENERATE group, COUNT(users_filter);

STORE result INTO 'Q3_output';