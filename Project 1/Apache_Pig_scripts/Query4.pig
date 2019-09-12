customer = LOAD '/user/hadoop/HW-1/customer.csv' USING PigStorage(',') as (id,name,age,gender,countrycode,salary);
transaction = Load '/user/hadoop/HW-1/transaction.csv' USING PigStorage(',') AS (transid,custid,transtotal,trandnumwitems,transdesc);
divide_customer = FOREACH customer GENERATE id,gender,(
			CASE
				WHEN age>=10 AND age<20 THEN '[10,20)'
				WHEN age>=20 AND age<30 THEN '[20,30)'
				WHEN age>=30 AND age<40 THEN '[30,40)'
				WHEN age>=40 AND age<50 THEN '[40,50)'
				WHEN age>=50 AND age<60 THEN '[50,60)'
				WHEN age>=60 AND age<=70 THEN '[60,70]'
			END
			)AS age_range;
join_c_t = JOIN divide_customer BY id, transaction BY custid;
divide_c_t = GROUP join_c_t BY (age_range, gender); 
result = FOREACH divide_c_t GENERATE ($0.$0) AS AgeRange,($0.$1) AS Gender,MIN ($1.$5) AS MinTransTotal, MAX($1.$5) AS MaxTransTotal, AVG($1.$5) AS AvgTransTotal;
DUMP result;
