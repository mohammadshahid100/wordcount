/*

Finding the top N products with the highest sales amount
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem13 {
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
    salesData.show()

    val highest = salesData.orderBy($"salesAmount".desc).limit(3).alias("top 3 product highest sale").show()


  }

}
