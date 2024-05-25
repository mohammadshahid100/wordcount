/*
you have a row that is too long to show horizontally also the text get truncated
how can u show the data vertically and also show full text?


package org.apache.spark.sql
object AccessShowString {
  def showString[T](df: Dataset[T],
                    _numRows: Int, truncate: Int = 20, vertical: Boolean = false): String = {
    df.showString(_numRows, truncate, vertical)
  }
}


object Problem9 {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("Problem-6").master("local[*]").getOrCreate()

    import spark.implicits._

    val data = Seq((100, "Steven", "king", "steven.king@example.com", 123456789, "2003-06-17", "Ad_Pres", 24000, "null", "null", 90))
      .toDF("employee_id", "first_name", "last_name", "email_id",
        "phone_number", "hire_date", "job_id", "salary", "commission_pct", "manager_id", "department_id")

    // Adjust column width to display full text
    spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 100)
    spark.conf.set("spark.sql.repl.eagerEval.truncate", false)

    // Display the DataFrame vertically and full text
    println(showString(data, 100))
  }
  }


 */