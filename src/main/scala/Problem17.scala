/*
GGiven a DataFrame df with columns first_name and last_name, create a new column full_name by concatenating the
two columns. However, if either first_name or last_name is null, set full_name to "Unknown".
 */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object problem17 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._
    // Sample data
    val data = Seq(
      ("John", "Doe"),
      ("Jane", null),
      (null, "Smith"),
      (null, null)
    )

    // Create DataFrame
    val df = spark.createDataFrame(data).toDF("first_name", "last_name")

    val fullName = df.withColumn("full_Name", when($"first_name".isNull || $"last_name".isNull, "unknown")
      .otherwise(concat($"first_name", $"last_name")))
    fullName.show()


  }

}
