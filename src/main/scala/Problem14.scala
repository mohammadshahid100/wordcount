/*
Finding the cumulative sum of sales amount for each product.
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem14 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WordCountExample")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val salesData = Seq(
      ("Product1", 100),
      ("Product2", 200),
      ("Product3", 150),
      ("Product4", 300),
      ("Product5", 250),
      ("Product6", 180)).toDF("Product", "SalesAmount")

    val windowSpec = Window.orderBy("Product")

    val comSum = salesData.withColumn("comSum", sum("SalesAmount").over(windowSpec).alias("comSUm"))
    val comSum1 = salesData.select(sum("SalesAmount").over(windowSpec).alias("comSUm"))
    comSum.show()

  }

}
