/* find the employees who earn more than their managers in spark .
SELECT e.employee_id, e.first_name, e.salary,e.manager_id
FROM employees e
INNER JOIN employees m
ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;

 */


import org.apache.spark.sql.SparkSession

object Problem5 {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("Problem-2").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employees = Seq((1, "John Doe", 60000.0, 3),
      (2, "Jane Smith", 95000.0, 3),
      (3, "Tom Jhonson", 90000.0, 4),
      (4, "Alice Brown", 1200000.0, 5),
      (5, "Bob white", 80000.0, 4),
      (6, "Eva Devis", 65000.0, 3)
    ).toDF("employee_id", "employee_name", "salary", "manager_id")

    val joinedDF = employees.alias("emp").join(employees.alias("mgr"),
      col("emp.manager_id") === col("mgr.employee_id"), "inner")
    joinedDF.show()

    val filterDf = joinedDF.filter(col("emp.salary") > col("mgr.salary"))
    filterDf.show()

  }

  }
