import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Given a list of integers, write a Scala Spark program to classify each integer as
either "Even" or "Odd" using if-else statements.
 */

object Problem20 {
  def main(args:Array[String]) ={
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._

    val numbers = List(1,2,3,4,5,6,7,8,9)
    val rdd = spark.sparkContext.parallelize(numbers)
    val rdd1 = rdd.map(num => {if (num%2 == 0)(num, "even") else (num, "odd")})
    rdd1.collect().foreach(println)

    val df = numbers.toDF("number")
    val df1 = df.withColumn("oddOrEven", when($"number" %2 === 0, "even").otherwise("odd")).show()


  }

}
