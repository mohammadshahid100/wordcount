import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*

Task - Write a PySpark program to select every 3rd (nth) row in the dataset
 */


object Problem24 {
  def main (args:Array[String])={


    val spark = SparkSession.builder
    .appName("CreateDataFrame")
      .master("local[*]")
    .getOrCreate()

    import spark.implicits._

    // Define schema  for the DataFrame

    val data = Seq(
    (1001, "John Doe", 50000),
    (2001, "Jane Smith", 60000),
    (1003, "Michael Johnson", 75000),
    (4000, "Emily Davis", 55000),
    (1005, "Robert Brown", 70000),
    (6000, "Emma Wilson", 80000),
    (1700, "James Taylor", 65000),
    (8000, "Olivia Martinez", 72000),
    (2900, "William Anderson", 68000),
    (3310, "Sophia Garcia", 67000)).toDF("emp_id", "name", "salary")

    val dfWithIndex = data.withColumn("index", org.apache.spark.sql.functions.monotonically_increasing_id())
    val result = dfWithIndex.filter($"index" %3 === 0).show()
    spark.stop()
  }

}
