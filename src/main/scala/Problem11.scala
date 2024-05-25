/*

Finding the count of orders placed by each customer and the total order amount for each customer.
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Problem11 {


  def main(args: Array[String])={

    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._
    val orderData = Seq(
      ("Order1", "John", 100),
      ("Order2", "Alice", 200),
      ("Order3", "Bob", 150),
      ("Order4", "Alice", 300),
      ("Order5", "Bob", 250),
      ("Order6", "John", 400)
    ).toDF("OrderID", "Customer", "Amount")
    orderData.show()
    val countOrder = orderData.groupBy("Customer").agg(count("*").alias("total count")).show()
    val totalOrder = orderData.groupBy("Customer").agg(sum("Amount").alias("total amount")).show()



  }

}
