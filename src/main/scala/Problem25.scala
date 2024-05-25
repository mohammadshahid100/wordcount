import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
If a user has only one activity, return that activity. If a user has multiple activities, display the second most recent one.
Note that a user cannot perform more than one activity at the same time.

 */


object Problem25 {
  def main(args:Array[String]) ={

    val spark = SparkSession.builder
      .appName("CreateDataFrame")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val data = Seq(
      ("Alice", "Travel", "2020-02-12", "2020-02-20"),
      ("Alice", "Dancing", "2020-02-21", "2020-02-23"),
      ("Alice", "Travel", "2020-02-24", "2020-02-28"),
      ("Bob", "Travel", "2020-02-11", "2020-02-18")
    )

    val df = spark.createDataFrame(data).toDF("username", "activity", "startDate", "endDate")

    // Convert date strings to DateType
    val dfWithDates = df.withColumn("startDate", col("startDate").cast("date"))
      .withColumn("endDate", to_date(col("endDate")))

    // Define window specification
    val windowSpec = Window.partitionBy("username").orderBy(col("startDate").desc)

    // Rank activities within each user group based on start date
    val rankedDF = dfWithDates.withColumn("activity_rank", row_number().over(windowSpec))

    // Filter to get the desired result
    val resultDF = rankedDF.filter(
      col("activity_rank") === 2 ||
        (col("activity_rank") === 1 )
    ).drop("activity_rank")

    // Show result
    resultDF.show()
    spark.stop()
  }

}
