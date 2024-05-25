/*
Given a DataFrame df with columns sales, expenses, and profit, create a new column profit_status based on the
following conditions:If profit is greater than 0, set profit_status to "Profitable".If profit is equal to 0,
set profit_status to "Break-even".If profit is less than 0 and sales is greater than 0, set profit_status to
"Loss-making".If profit is less than 0 and sales is 0, set profit_status to "No Sales".
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem21 {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1000, 800, 200),
      (2000, 1800, 200),
      (0, 1000, -1000),
      (500, 400, 100),
      (500, 400, 0),
      (500, 400, -200)

    )

    val df = spark.createDataFrame(data).toDF("sales", "expenses", "profit")
    val result = df.withColumn("profit_status", when($"profit" > 0, "Profitable")
      .when($"profit" === 0, "Break-even").when($"profit" < 0 && $"sales" > 0, "Loss-making")
      .otherwise("No Sales"))
    result.show()


  }

  }
