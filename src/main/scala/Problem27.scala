/*
Challenge: How to keep only the top 2 most frequent values as it is and replace everything else as ‘Other’
 */

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Problem27  extends  App{

  val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
  import spark.implicits._


  val data = Seq(
    ("John", "Engineer"),
     ("John", "Engineer"),
     ("Mary", "Scientist"),
     ("Bob", "Engineer"),
     ("Bob", "Engineer"),
     ("Bob", "Scientist"),
     ("Sam", "Doctor")
  ).toDF("name", "job")
  val windowSpec = Window.partitionBy("job")
  val result = data.withColumn("cnt", count("*") over(windowSpec))
    .withColumn("rank", dense_rank() over(Window.orderBy(col("cnt").desc)))
    .withColumn("job", when(col("rank") > 2, "other").otherwise($"job")).show()



}
