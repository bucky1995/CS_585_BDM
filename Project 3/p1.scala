import org.apache.spark.sql.SparkSession
import java.io.File
// For implicit conversions like converting RDDs to DataFrames

object p1{
    def main(args: Array[String]){
        val spark = SparkSession
            .builder()
            .appName("Project3P1")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        val transaction_path = "file:///home/mqp/Desktop/Project3/transaction.csv"
        val transaction_columns = Seq("transid", "customerid", "transtotal", "transnumitems","transdesc")
        val transaction_matrix = spark.read.format("com.databricks.spark.csv")
            .option("header","false")
            .option("inferSchema", "true")
            .load(transaction_path).toDF(transaction_columns:_*)
        transaction_matrix.createOrReplaceTempView("transaction")
        val t1 = transaction_matrix.filter($"transtotal" > 200)
        t1.show()

        t1.createOrReplaceTempView("t1")
        val t2= spark.sql("SELECT transnumitems, COUNT(transid), SUM(transtotal), MIN(transtotal), MAX(transtotal)," +
          " AVG(transtotal) FROM t1 GROUP BY transnumitems")
        t2.show()
	t2.write.csv("file:///home/mqp/Desktop/Project3/CSV/t2.csv")
	spark.catalog.dropTempView("t2")

	val t3 = spark.sql("SELECT customerid, COUNT(transid) AS count FROM t1 GROUP BY customerid")
        t3.show()

	
        val t4 =  transaction_matrix.filter($"transtotal" < 600)
        t4.show()

        t4.createOrReplaceTempView("t4")
        val t5 = spark.sql("SELECT t4.customerid, COUNT(transid) AS count FROM t4 GROUP BY t4.customerid")
	t5.show()
	spark.catalog.dropTempView("t1")
        t3.createOrReplaceTempView("t3")
        t5.createOrReplaceTempView("t5")
	spark.catalog.dropTempView("t4")

        val t6 = spark.sql("SELECT t5.customerid from t3, t5 where t5.count*3 < t3.count")
	spark.catalog.dropTempView("t3")
	spark.catalog.dropTempView("t5")
        t6.show()
	t6.write.csv("file:///home/mqp/Desktop/Project3/CSV/t6.csv")
    }
}



