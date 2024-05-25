/*
Write a query to find out the customers who booked both air and train tickets.
My solution
 */
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object Problem26 extends App {

  val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
  val data =Seq(Row(1, 101, "air"),
    Row(2, 101, "train"),
    Row(3, 102, "air"),
    Row(4, 103, "train"),
    Row (5, 104, "air"),
    Row (6, 104, "train"),
    Row (7, 105, "train") )
  val schema = StructType(Seq(
    StructField("booking_id", IntegerType, nullable = true),
   StructField("customer_id", IntegerType, nullable = true),
   StructField("ticket_type", StringType, nullable = true)))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // using normal way
 /* val result = df.groupBy("customer_id").agg(countDistinct("ticket_type").alias("ticket_count"))
    .filter(col("ticket_count") === 2).select("customer_id").show()

  */

  // using window function

  /* val windsowSpec = Window.partitionBy("customer_id").orderBy("ticket_type")
  val result = df.withColumn("row", row_number() over(windsowSpec))
    .filter(col("row") === 2).select("customer_id").show()
   */


  // using spark sql

  df.createOrReplaceTempView("bookings")

  spark.sql(
    """
   select customer_id from(
   select customer_id, count(distinct(ticket_type)) as cnt from bookings
   where ticket_type in ('air', 'train') group by customer_id
   ) where cnt = 2
  """).show()


}
