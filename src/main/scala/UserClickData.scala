import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object UserClickData extends App{

  val spark = SparkSession
    .builder()
    .appName("UserClicksETL")
    .master("local[*]")
    .getOrCreate()

  val jsonFilePath = "C:/Users/HP ELITEBOOK/Desktop/data.json"
  val rawDF = spark.read.json(jsonFilePath)

  // Assuming a predefined schema
  val schema = StructType(Seq(
    StructField("click_event_id", StringType, nullable = true),
    StructField("user_id", LongType, nullable = true),
    StructField("url", StringType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("ip_address", StringType, nullable = true),
    StructField("device", StringType, nullable = true),
    StructField("browser", StringType, nullable = true)
  ))

  // Apply schema and convert timestamp format
  val rawDFWithSchema = spark.read.schema(schema).json(jsonFilePath)

  rawDFWithSchema.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd") )

  val outputDF = rawDFWithSchema
    .groupBy("url", "country", "date")
    .agg(
      avg(col("time_spent")).alias("avg_time_spent"),
      countDistinct(col("user_id")).alias("unique_users"),
      count(col("click_event_id")).alias("clicks")
    )
  val outputPath = "C:/Users/HP ELITEBOOK/Desktop/output.csv"
  outputDF
    .write
    .mode("overwrite")
    .csv(outputPath)

}
