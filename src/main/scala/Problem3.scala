/* Question (From Datalemur):
Assume you"re given a table Twitter tweet data, write a query to obtain a histogram of tweets posted per user in 2022.
Output the tweet count per user as the bucket and the number of Twitter users who fall into that bucket.
In other words, group the users by the number of tweets they posted in 2022 and count the number of users in each group.

 */

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Problem3 {

  def main(args:Array[String])={

    val spark = SparkSession.builder().appName("Problem-2").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Define the input data
    val data = Seq(
      (214252, 111, "Am considering taking Tesla private at $420. Funding secured.", "12/30/2021 00:00:00"),
      (739252, 111, "Despite the constant negative press covfefe", "01/01/2022 00:00:00"),
      (846402, 111, "Following @NickSinghTech on Twitter changed my life!", "02/14/2022 00:00:00"),
      (241425, 254, "If the salary is so competitive why won’t you tell me what it is?", "03/01/2022 00:00:00"),
      (231574, 148, "I no longer have a manager. I can't be managed", "03/23/2022 00:00:00")
    )

//    // Define the schema
//    val schema = StructType(Array(
//      StructField("tweet_id", IntegerType, true),
//      StructField("user_id", IntegerType, true),
//      StructField("msg", StringType, true),
//      StructField("tweet_date", StringType, true)
//    ))

    // Create DataFrame
    var df = spark.createDataFrame(data).toDF("tweet_id", "user_id", "msg", "tweet_date")

    // Convert tweet_date to TimestampType
    df = df.withColumn("tweet_date", to_timestamp(col("tweet_date"), "MM/dd/yyyy HH:mm:ss"))

    // Filter tweets from 2022
    val df2022 = df.filter(year(col("tweet_date")) === 2022)

    // Group by user_id and count the number of tweets per user
    val tweetCountPerUser = df2022.groupBy("user_id").count()
//
//    // Group by tweet count and count the number of users per tweet count
    val histogram = tweetCountPerUser.groupBy("count")
  .agg(count("*").alias("users_num")).sort("count")
//
//    // Rename columns and show the histogram
    val result = histogram.withColumnRenamed("count", "tweet_bucket").withColumnRenamed("users_num", "users_num")
    result.show()


  }

}
