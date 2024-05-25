import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserClickData extends App{

  val spark = SparkSession
    .builder()
    .appName("UserClicksETL")
    .master("local[*]")
    .getOrCreate()

  val jsonFilePath = "C:/Users/HP ELITEBOOK/Desktop/data.json"
  val rawDF = spark.read.json(jsonFilePath)

  // Apply schema and convert timestamp format
//  val rawDFWithSchema = spark.read.schema(schema).json(jsonFilePath).show()

  val outputDF = rawDF
    .groupBy(col("url"), col("country"), date_format(col("timestamp"), "yyyy-MM-dd")
      .alias("event_date"))
    .agg(
      avg(minute(col("timestamp")).cast("int")).alias("avg_minute_spent"),
      countDistinct(col("user_id")).alias("unique_user_count"),
      count(col("click_event_id")).alias("click_count")
    )
  outputDF.show()
}
