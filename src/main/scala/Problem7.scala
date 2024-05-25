/*
 two consecutive student seats are exchanged in case of last value it will be same if it doesnt have any pair
input-
+---+-------+
| id|student|
+---+-------+
|  1|  Abbot|
|  2|  Doris|
|  3|Emerson|
|  4|  Green|
|  5| Jeames|
+---+-------+
 output-

---+-------+-------------+
| id|student|Exchange_seat|
+---+-------+-------------+
|  1|  Abbot|        Doris|
|  2|  Doris|        Abbot|
|  3|Emerson|        Green|
|  4|  Green|      Emerson|
|  5| Jeames|       Jeames|
+---+-------+-------------+
 */



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Problem7 {
  def main(args:Array[String]) ={

    val spark = SparkSession.builder().appName("Problem-6").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq((1, "Abbot"), (2, "Doris"), (3, "Emerson"), (4, "Green"), (5, "Jeames")).toDF("id", "student")
    data.show()
    val df_lead = data.withColumn("next_value", lead("student", 1)
      .over(Window.orderBy("id")))
    val df_lag = df_lead.withColumn("prev_value", lag("student", 1)
      .over(Window.orderBy("id")))
    val df_answer = df_lag.withColumn("Exchange_seat", when(col("id") % 2 =!= 0,
      coalesce(col("next_value"), col("student"))).otherwise(col("prev_value")))
    df_answer.select("id", "student", "Exchange_seat").show()




  }


}
