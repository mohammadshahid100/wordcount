import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
You are given a dataframe having columns order_date and order_amount. Your task is to find out cumulative sum of sales
 of each day partioned by month.

 */

object Problem23 {
  def main(args:Array[String])={

    val spark = SparkSession.builder().appName("").enableHiveSupport().master("local[*]").getOrCreate()
    import spark.implicits._

    val customer_orders = List(
      ("2022-01-01", 2000),
      ("2022-01-02", 2500),
      ("2022-01-03", 2100),
      ("2022-01-04", 2000),
      ("2022-02-01", 2200),
      ("2022-02-02", 2700),
      ("2022-02-03", 3000),
      ("2022-02-04", 1000),
      ("2022-02-05", 3000)
    ).toDF("order_date", "order_amount")

    val df = customer_orders.withColumn("order_date", col("order_date").cast("date"))
    val windowSpec = Window.partitionBy(month($"order_date")).orderBy("order_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val result = df.withColumn("cummulative Sum", sum("order_amount").over(windowSpec))
    result.show()

    //spark-sql

    df.createOrReplaceTempView("Orders")





  }

}
