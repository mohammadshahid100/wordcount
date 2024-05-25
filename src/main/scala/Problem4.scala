/* find top 5 customer with highest number of clicks

 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Problem4 {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("Problem-2").master("local[*]").getOrCreate()
    import spark.implicits._
    val data = Seq((1, "2023-01-01 12:00:00", "click"),
      (2, "2023-01-01 12:05:00", "view"),
      (1, "2023-01-01 12:10:00", "click"),
      (3, "2023-01-01 12:15:00", "view"),
      (2, "2023-01-01 12:20:00", "click"),
      (1, "2023-01-01 12:25:00", "view"),
      (3, "2023-01-01 12:30:00", "click"),
      (2, "2023-01-01 12:35:00", "view"),
      (1, "2023-01-01 12:40:00", "click"),
      (3, "2023-01-01 12:45:00", "view"),
      (1, "2023-01-02 12:10:00", "click"),
      (1, "2023-01-03 12:10:00", "click"),
      (1, "2023-01-04 12:10:00", "view")
    ).toDF("user_id", "timestamp", "interaction_type")

    data.filter(col("interaction_type") === "click").groupBy("user_id").count()
      .limit(5).show()
  }
}
