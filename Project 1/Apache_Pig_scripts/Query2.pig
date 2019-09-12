customer = LOAD '/user/hadoop/HW-1/customer.csv' USING PigStorage(',') as (id,name,age,gender,countrycode,salary);
transaction = Load '/user/hadoop/HW-1/transaction.csv' USING PigStorage(',') AS (transid,custid,transtotal,trandnumwitems,transdesc);

trans_group = GROUP transaction BY custid;
join_test = JOIN customer BY id, transaction BY custid;
join_test_group = GROUP join_test BY (customer::id, customer::name, customer::salary);
result_f = FOREACH join_test_group GENERATE ($0.$0) AS customer_ID, ($0.$1) AS name, ($0.$2) AS salary, COUNT($1) AS num_trans, SUM ($1.$8) AS total_sum, MIN ($1.$9) AS min_items;
DUMP result_f;


