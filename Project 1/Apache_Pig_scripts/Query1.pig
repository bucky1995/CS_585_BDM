customer = LOAD '/user/hadoop/HW-1/customer.csv' USING PigStorage(',') as (id,name,age,gender,countrycode,salary);
transaction = Load '/user/hadoop/HW-1/transaction.csv' USING PigStorage(',') AS (transid,custid,transtotal,trandnumwitems,transdesc);
join_test = JOIN customer BY id, transaction BY custid;
join_test_group = GROUP join_test BY (customer::id, customer::name, customer::salary);
result_f = FOREACH join_test_group GENERATE ($0.$1) AS name, COUNT($1) AS num_trans;
MIN = ORDER result_f BY num_trans ASC;
R = LIMIT MIN 1;
DUMP R;








