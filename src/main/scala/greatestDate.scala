/* Question- Suppose you are given a PySpark DataFrame df with two columns Date1 and Date2, representing dates in the
format 'dd-MMM-yy'. You need to find the greatest date between each row and add it as a new column named GreatestDate.
Write a PySpark script to achieve this.
 */



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object greatestDate {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ExamplesPractice").master("local[4]").getOrCreate()
    import spark.implicits._

    val df =List(("01-Feb-23", "01-Sep-23"),
    (" ","01-Dec- 23"),
    ("01-Mar- 23", " ")
    ).toDF("Date1", "Date2")

    val dfDates = df.withColumn("Date1", to_date(col("Date1"), "dd-MMM-YY"))
      .withColumn("Date2", to_date(col("Date2"), "dd-MMM-YY"))
    val great = dfDates.withColumn("greatestDate", greatest(col("Date1"), col("Date2")))
    great.show()





  }

}
