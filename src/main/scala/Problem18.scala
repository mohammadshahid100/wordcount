/*
Write a solution to replace the null values of the drink with the name of the drink of the previous row that is not null.
 It is guaranteed that the drink on the first row of the table is not null.
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem18 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._
    val data = Seq(
      (1, "Orange Margarita"),
      (2, null),
      (3, "St Germain Spritz"),
      (6, null),
      (7, null),
      (9, "Rum and Coke")
    ).toDF("id", "drink")

    val windowSpec = Window.orderBy("id")
    val result = data.withColumn("drink", coalesce($"drink", lag("drink", 1).over(windowSpec))).show()



  }

}
