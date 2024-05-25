// read the 3rd quarter 25% of file n dataframe

import org.apache.spark.sql.SparkSession

object Problem8 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Problem-6").master("local[*]").getOrCreate()
    import spark.implicits._

    val data  = Seq(("Alice", 28), ("Bob", 35), ("Charlie", 42), ("David", 25),("Eva", 31), ("Frank",38), ("Grace",45), ("Henry",29))
      .toDF("name", "age")
    val rowCount = data.count()
    val startRow = (rowCount * 0.5).toInt
    val endRow = (rowCount * 0.75).toInt
    val df = data.sample(false, 0.25).limit(endRow - startRow)
    df.show()

  }
  }
