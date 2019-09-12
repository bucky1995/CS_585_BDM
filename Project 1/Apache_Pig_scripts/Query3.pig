customer = LOAD '/user/hadoop/HW-1/customer.csv' USING PigStorage(',') as (id,name,age,gender,countrycode,salary);
customer_country_group = GROUP customer BY (countrycode);
customer_country_group_count = FOREACH customer_country_group GENERATE group as customer, COUNT($1) AS count;
FILTER_final = FILTER customer_country_group_count BY ((count>5000) OR (count<2000));
DUMP FILTER_final



