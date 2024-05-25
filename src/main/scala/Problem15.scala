import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Problem15 {
  def main(args:Array[String]) ={
        val spark = SparkSession.builder()
          .appName("MaxRevenueByCategory")
          .master("local[*]")
          .getOrCreate()

        // Define sales data
        val salesData = Seq(
          ("Product1", "Category1", 100),
          ("Product2", "Category2", 200),
          ("Product3", "Category1", 150),
          ("Product4", "Category3", 300),
          ("Product5", "Category2", 250),
          ("Product6", "Category3", 180)
        )

        // Create DataFrame
        val salesDF = spark.createDataFrame(salesData)
          .toDF("Product", "Category", "Revenue")

        // Find maximum revenue for each category
        val maxRevenuePerCategory = salesDF.groupBy("Category")
          .agg(max("Revenue").alias("MaxRevenue"))

        // Join with original DataFrame to get corresponding product
        val maxRevenueData = maxRevenuePerCategory.join(salesDF, Seq("Revenue"), "inner")
          .select(salesDF("Product"),
            maxRevenuePerCategory("Category"),
            maxRevenuePerCategory("MaxRevenue"))

        // Show the result
        maxRevenueData.show()

  }

}
