/* Q:Write a solution to find the IDs of the users who visited without making any transactions and the number of times
they made these types of visits.
 */

import org.apache.spark.sql._

object Problem2 {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("Problem-2").master("local[*]").getOrCreate()
    import spark.implicits._

    val visitsData = Seq((1, 23),
      (2, 9),
      (4, 30),
      (5, 54),
      (6, 96),
      (7, 54),
      (8, 54))
      .toDF("visit_id", "customer_id")

    val transactionsData = Seq((2, 5, 310),
      (3, 5, 300),
      (9, 5, 200),
      (12, 1, 910),
      (13, 2, 970))
      .toDF("transaction_id", "visit_id","amount")
//     visitsData.show()
//    transactionsData.show("visitor-without-making-any-transactiom", )

    val joindf = visitsData.join(transactionsData, Seq("visit_id"), "left_anti")
    val groupedData = joindf.groupBy("customer_id").count()
      .withColumnRenamed("count", "count_no_trans").show()



  }


}
