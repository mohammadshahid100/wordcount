import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dataframecode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ExamplesPractice").master("local[4]").getOrCreate()
//    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
//      .option("path", "C:/Users/HP ELITEBOOK/Desktop/data1.csv").load()
//    df.show()
//    df.printSchema()

    import spark.implicits._
//    val df = List(("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Chahid", 17)).toDF("name", "age")
//    val transForDf = df.filter(col("name").startsWith("Cha"))
//    transForDf.show()
//    val df = Seq(("Alice", "Engineer"), ("Bob", "Developer"), ("Charlie", "Manager")).toDF("name", "role")
//    val transDf = df.filter(col("role").like("%Eng%"))
//    transDf.show()



//      .when(col("age") < 18, "child")
//      .otherwise("old").as("category"))
//    transForDf.show()

//    val df = Seq(("Alice", 25, 95), ("Bob", 30, 65), ("Charlie", 35, 45)).toDF("name", "age", "marks")
//    val transForDf = df.select(col("name"), col("age"),
//      when(col("marks") > 85, "first class").when(col("marks") > 50, "Average")
//        .otherwise("pass").as("category"))
//    transForDf.show()
//    transForDf.printSchema()

//    val df = Seq(("A", 40, 200), ("B", 30, 150), ("C", 20, 250)).toDF("product_id", "quantity", "Price")
//
//    val transDf = df.select(col("*"), when(col("product_id") === "null" &&
//      col("quantity") < 0 && col("Price") < 0, 0).as("category").otherwise("Price"))
//      .groupBy("product_id").agg(sum(col("quantity") * col("Price")).alias("totalRevenue"))
//      transDf.show()

//    val salesData = List(
//      ("Product1", "Category1", 100),
//      ("Product1", "Category2", 50),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//    val wind = Window.partitionBy("Product").orderBy("Product")
//    val runnTotal = salesData.withColumn("max revenue", max("Revenue").over(wind))
//    val result = runnTotal.show()

    // 4.Calculating the percentage contribution of each product's revenue to the total revenue in its category
//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category1", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category2", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category2", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//    val WinSpec = Window.partitionBy("Category")
//    val revenCategory = sum("Revenue").over(WinSpec)
//    val contributionPercen = salesData.withColumn("TotalRevenue", revenCategory)
//      .withColumn("contribution", col("Revenue")/col("TotalRevenue") *100)
//    contributionPercen.show()


    // lead and lag
//    val salesData = List(
//            ("ProductA", "2023-09-01", 100.0),
//            ("ProductA", "2023-09-02", 120.0),
//            ("ProductA", "2023-09-03", 130.0),
//            ("ProductB", "2023-09-01", 200.0),
//            ("ProductB", "2023-09-02", 210.0)
//        ).toDF("Product", "date", "revenue")
//    val wind = Window.partitionBy("Product").orderBy("date")
//    val df = salesData.withColumn("lead", lead(col("date"), 1).over(wind))
//    df.show()
//    val df1 = salesData.withColumn("lag", lag(col("date"), 1).over(wind))
//    df1.show()

    // Prob- we want to find the difference between price on each day with its previous day

//    val data = List((1, "kitkat", 1000.0, "2021-01-01"),
//                    (1, "kitkat", 2000.0, "2021-01-02"),
//                    (1, "kitkat", 1000.0, "2021-01-03"),
//                    (1, "kitkat", 2000.0, "2021-01-04"),
//                    (1, "kitkat", 3000.0, "2021-01-05"),
//                    (1, "kitkat", 1000.0, "2021-01-06")).toDF("IT_ID", "IT_NAME", "Price", "PriceDate")
//    val win = Window.orderBy("PriceDate")
//    val dif = data.withColumn("diff", col("Price") - lag("Price", 1).over(win))
//    dif.show()

    // calculate difference in daily sales for each product considering product are grouped by category
    // val data = List(("ProductA", "CategoryX", "2023-09-01", 100.0),
    //                    ("ProductA", "CategoryX", "2023-09-01", 120.0),
    //                    ("ProductA", "CategoryX", "2023-09-01", 130.0),
    //                    ("ProductB", "CategoryY", "2023-09-01", 200.0),
    //                    ("ProductB", "CategoryY", "2023-09-01", 210.0)).toDF()

    // If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"
    // schema- ID,Name,Salary, Date

//    val data = List((1, "John", 1000, "01/01/2016"),
//                  (1, "John", 2000, "02/01/2016"),
//                  (1, "John", 1000, "03/01/2016"),
//                  (1, "John", 2000, "04/01/2016"),
//                  (1, "John", 3000, "05/01/2016"),
//                  (1, "John", 1000, "06/01/2016")).toDF("ID", "Name", "Salary", "Date")
//    val win = Window.orderBy("Date")
//    val df = data.withColumn("new", lead("Salary", 1).over(win)).withColumn("new1",
//      when(col("salary") > col("new"), "UP").otherwise("DOWN"))
//    df.show()

    // calculate the percentage change in salary from the previous row to the current row order by id using same sample data
//    val data = List((1, "Alice", 1000),
//        (2, "Bob", 2000),
//        (3, "Charlie", 1500),
//        (4, "David", 3000)).toDF("id", "Name", "salary")
//
//    val file = spark.read.format("csv")
//      .option("path", "C:/Users/HP ELITEBOOK/Desktop/work/data4.csv").load.toDF("Name", "City")
//    file.show()
//    val result = file.select($"Name", explode(split($"Hobbies", "\\s+"))
//      .alias("Hobbies"))
//    result.show()
//    val output = file.write.format("csv").mode(SaveMode.Append)
//  .partitionBy("City")
//      .option("path", "C:/Users/HP ELITEBOOK/Desktop/work/output1").save()

    // 2. Calculating the average rating for each user based on the last 3 ratings.
//    val ratingData = Seq(
//      ("User1", "Movie1", 4.5),
//      ("User1", "Movie2", 3.5),
//      ("User1", "Movie3", 2.5),
//      ("User1", "Movie4", 4.0),
//      ("User1", "Movie5", 3.0),
//      ("User1", "Movie6", 4.5),
//      ("User2", "Movie1", 3.0),
//      ("User2", "Movie2", 4.0),
//      ("User2", "Movie3", 4.5),
//      ("User2", "Movie4", 3.5),
//      ("User2", "Movie5", 4.0),
//      ("User2", "Movie6", 3.5)
//    ).toDF("User", "Movie", "Rating")
//    ratingData.show()
//    val windowSpec = Window.partitionBy("User").orderBy(asc("Movie")).rowsBetween(-2, 0)
//    val result = ratingData.withColumn("AverageRating", avg("Rating").over(windowSpec)).show()

//    3. Finding the maximum revenue for each product category and the corresponding product.

//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180)
//    ).toDF("Product", "Category", "Revenue")
//    salesData.show()
//
//    val widowSpec = Window.partitionBy("Category").orderBy("Product")
//    val revForEachProd = salesData.withColumn("MaxRevenueForEachProductCategory",
//      max("Revenue").over(widowSpec)).show()

//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category1", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category2", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category2", 180)
//    ).toDF("Product", "Category", "Revenue")
//    salesData.show()
//
//    val windowSpec = Window.partitionBy("Category")
//    val totalRev = sum("Revenue").over(windowSpec)
//    val cont = salesData.withColumn("totalRev", totalRev)
//      .withColumn("Contribution", col("Revenue") / col("totalRev") *100)
//    cont.show()

    // Write a code to find out the employeename who has not assigned any project, and display
    // "-No Project Assigned"( tables :- [EmployeeDetail],[ProjectDetail]).
    //Write a  code to find out the project name which is not assigned to any
    // employee( tables :- [EmployeeDetail],[ProjectDetail]).

    val data = Seq(Row(1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"),
      Row(2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"),
      Row(3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"),
      Row(4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"),
      Row(5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"))


    // Create a schema  for the DataFrame

    val schema = StructType(Seq(StructField("EmployeeID", IntegerType, nullable = true),
    StructField("First_Name", StringType, nullable = true),
      StructField("Last_Name", StringType, nullable = true),
      StructField("Salary", DoubleType, nullable = true),
      StructField("Joining_Date", StringType, nullable = true),
      StructField("Department", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true)))

    val pro_data = Seq(Row(1, 1, "Task Track"),
      Row(2, 1, "CLP"),
      Row(3, 1, "Survey Management"),
      Row(4, 2, "HR Management"),
      Row(5, 3, "Task Track"),
      Row(6, 3, "GRS"),
      Row(7, 3, "DDS"),
      Row(8, 4, "HR Management"),
      Row(9, 6, "GL Management"))

    val pro_schema = StructType(Seq(
      StructField("Project_DetailID", IntegerType, nullable = true),
      StructField("Employee_DetailID", IntegerType, nullable = true),
      StructField("Project_Name", StringType, nullable = true)))

    val empdf = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    val pro_df = spark.createDataFrame(spark.sparkContext.parallelize(pro_data), pro_schema)
    val joinedCond = empdf("EmployeeID") === pro_df("Employee_DetailID")
    val joinedDf = empdf.join(pro_df, joinedCond, "outer")
    joinedDf.show()
    val result1 = joinedDf.filter(col("Employee_DetailID").isNull)
      .select(col("First_Name"), col("Last_Name"), lit("No Project Assigned").alias("remark"))
    result1.show()

    val result2 = joinedDf.filter(col("EmployeeID").isNull)
      .select(col("Project_DetailID"), col("Project_Name"))
    result2.show()


  }

}
