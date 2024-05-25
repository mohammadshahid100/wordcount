/*
You are given a dataframe having columns order_id, cust_id, order_date, order_amount.
You need to find out new and repeat customers count for each day.

output-
+----------+--------------+----------------+
|order_date|new_customers|repeat_customers|
+----------+--------------+----------------+
|2022-01-01|             3|               0|
|2022-01-03|             1|               2|
|2022-01-02|             2|               1|
+----------+--------------+----------------+

 */


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object Problem10 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder.master("local[*]").appName("Problem-9").getOrCreate()
    import spark.implicits._
    val df = Seq(("Alice mnm,123", 25), ("Bob hsjdhs,456", 30), ("Charlie jshgd,789", 35)).toDF("name", "age")
    val transformedDF = df.withColumn("firstName", split(col("name"), " ").getItem(0))
      .withColumn("lastName", split(col("name"), " ").getItem(1))
    transformedDF.show()


  }

}
