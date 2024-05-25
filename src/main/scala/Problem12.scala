/*
Question 4. Finding the count of occurrences for each word in a text document.
 */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}


object Problem12 {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("WordCountExample")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val textData = Seq(
      "Hello, how are you?",
      "I am fine, thank you!",
      "How about you?").toDF("Text")
    textData.show()

    val df = textData.select(explode(split($"Text", "\\s+")).alias("words"))
      df.groupBy("words").agg(count("*").alias("total count of each word")).show()
  }

}
