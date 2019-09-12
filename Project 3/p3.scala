import org.apache.spark.sql.SparkSession
import java.io.File
// For implicit conversions like converting RDDs to DataFrames

object p3{
    def main(args: Array[String]){
        val spark = SparkSession
            .builder()
            .appName("Project3P1")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        val n_path = "file:///home/mqp/Desktop/Project3/p3/N.csv"
        val n_columns = Seq("i", "j", "Mij")
        val m_path = "file:///home/mqp/Desktop/Project3/p3/M.csv"
        val m_columns = Seq("j", "k", "Njk")

        val n_matrix = spark.read.format("com.databricks.spark.csv")
          .option("header","false")
          .option("inferSchema", "true")
          .load(n_path).toDF(n_columns:_*)
        val m_matrix = spark.read.format("com.databricks.spark.csv")
          .option("header","false")
          .option("inferSchema", "true")
          .load(m_path).toDF(m_columns:_*)

        n_matrix.createOrReplaceTempView("n")
	n_matrix.show()
        m_matrix.createOrReplaceTempView("m")
	val temp = spark.sql("SELECT n.i, m.j, n.Mij*m.Njk AS val FROM n,m" +
          " WHERE n.j=m.j")
	temp.show()
	temp.createOrReplaceTempView("temp")

        val result = spark.sql("SELECT i, j, SUM(val) FROM temp GROUP BY i,j")
        result.show()



    }
}



