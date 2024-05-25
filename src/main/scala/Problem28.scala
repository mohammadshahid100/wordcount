import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem28 extends  App{

  val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
  val data = Seq(
    ("2023-04-10", 1, 100),
    ("2023-04-11", 2, 200),
    ("2023-04-12", 3, 300),
    ("2023-04-13", 1, 400),
    ("2023-04-14", 2, 500)
  )

  import spark.implicits._

  // Create DataFrame from sample data
  val orders = data.toDF("order_date", "customer_id", "amount")

  val result = orders.withColumn("dayofweek",
    when(dayofweek(col("order_date")) === 1 || dayofweek(col("order_date")) === 7, "weekends")
      .otherwise("weekdays"))
  result.show()


}
