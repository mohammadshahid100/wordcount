import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Find The Team Size Problem
Write an Pyspark code to find the team size of each of the employees.
Return result table in any order.
 */


object Problem19 {
  def main(args:Array[String]) ={
    val spark = SparkSession.builder().appName("").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
    (1, 8),
    (2, 8),
    (3, 8),
    (4, 7),
    (5, 9),
    (6, 9)).toDF("employee_id", "team_id")

    val teamSize = data.groupBy("team_id").agg(count("*").alias("team_size"))
    val resut = teamSize.join(data, Seq("team_id"), "inner").orderBy("employee_id").show()




  }


}
