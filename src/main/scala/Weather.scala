/*
Given a DataFrame df with columns temperature (in Celsius) and humidity, create a new column weather_type
with the following conditions:If temperature is greater than or equal to 30 and humidity is greater than or
equal to 60, set weather_type to "Hot and Humid".If temperature is less than 30 and humidity is greater than
or equal to 60, set weather_type to "Warm and Humid".If temperature is greater than or equal to 30 and humidity is
less than 60, set weather_type to "Hot and Dry".If temperature is less than 30 and humidity is less than 60,
set weather_type to "Moderate".

 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Weather {

  def main(args:Array[String])= {
     val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq((35, 65), (37, 68), (28, 65), (25, 68),(35, 55), (37, 52), (19, 45), (9, 23),(9, 0)).toDF("temp", "humid")
      data.na.fill(80)
    val df = data.withColumn("weather_type", when(($"temp" >= 30) &&  ($"humid" >= 60), "Hot and Humid")
      .when($"temp" < 30 && $"humid" >= 60,"Warm and Humid")
      .when($"temp" >= 30 && $"humid" < 60, "Hot and Dry").otherwise("Moderate"))
    df.show()



  }

}
