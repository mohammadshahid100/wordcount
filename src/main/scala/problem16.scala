/*

Given a DataFrame df with columns product_id, quantity, and price, calculate the total revenue for each product.
 However, if the product_id is null or the quantity or price is negative, set the revenue to 0.Combining Multiple Conditions:
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object problem16 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1, 10, 100.0),
      (2, -5, 200.0),
      (3, 15, 150.0),
      (null.asInstanceOf[Int], 20, 50.0)
    )

    // Create DataFrame
    val df = spark.createDataFrame(data).toDF("product_id", "quantity", "price")
    val revenue = df.withColumn("total_revenue",
      when($"product_id".isNull || $"quantity" < 0 || $"price" < 0, 0)
        .otherwise($"quantity" * $"price"))
    revenue.show()

  }

}
