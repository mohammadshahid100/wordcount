// find missing number using spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Problem6 {

  def main(args:Array[String]) ={

    val spark = SparkSession.builder().appName("prpblem-5").master("local[*]").getOrCreate()
    import spark.implicits._
    val df = Seq(1,2,3,6,7,8).toDF("id")
    val maxId = df.agg(max("id")).head().getInt(0)
    val minId = df.agg(min("id")).head().getInt(0)
    val genratedf = (minId to maxId).toDF("id")
    val joinedDf = genratedf.join(df, Seq("id"), "left_anti")
      .withColumnRenamed("id", "missingNumber")
    joinedDf.show()





  }

}
