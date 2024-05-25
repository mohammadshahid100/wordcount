/*
Given a DataFrame df with columns sales and expenses, create a new column profit_status based on the following
conditions:If sales and expenses are both greater than 0 and sales is greater than expenses, set profit_status to
"Profitable".If sales and expenses are both greater than 0 and sales is equal to expenses, set profit_status to
"Break-even".If sales is greater than 0 and expenses is 0, set profit_status to "No Expenses".If sales and expenses
are both greater than 0 and expenses is greater than sales, set profit_status to "Loss-making".For all other cases,
set profit_status to "Unknown".Categorizing Data with Complex Conditions:


 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

object Problem22 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._
    // Sample DataFrame
    val data = Seq(
      (1000, 800),
      (2000, 1800),
      (0, 1000),
      (500, 400),
      (1200, 1200),
      (200, 0),
      (800, 1200)
    )

    val df = spark.createDataFrame(data).toDF("sales", "expenses")

    val df1 = df.withColumn("profit_status",
      when($"sales" > 0 && $"expenses" > 0 && $"sales" > $"expenses", "profitable")
      .when($"sales" > 0 && $"expenses" > 0 && $"sales" === $"expenses", "Break-even")
        .when($"sales" > 0 && $"expenses" === 0, "No Expenses")
        .when($"sales" > 0 && $"expenses" > 0 && $"sales" < $"expenses", "Loss-making")
        .otherwise("Unknown"))
    df1.show()
  }

  }
