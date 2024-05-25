import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem29 extends App{

  val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
  import spark.implicits._

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

      val df = List(("2023-10-07", "2024-04-22")).toDF("date1", "date2")
      val dateDiffDf = df.withColumn("days_diff", ($"date2"))
      dateDiffDf.show()


    }
