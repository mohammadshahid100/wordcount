import org.apache.spark.SparkContext

object wordcount {


  def main(args: Array[String]): Unit = {

    val sc =new SparkContext("local[*]", "sparkcount")
    val rdd = sc.parallelize(Array("apple", "orange", "banana", "pear", "graps"))
    val searchword = "ap"
    val result = rdd.filter(x => x.contains(searchword)).count()
    println(result)


//    val rdd1 = sc.parallelize(Array((1, "apple"), (2, "banana"), (3, "orange")))
//    val rdd2 = sc.parallelize(Array((1, "red"), (2, "yellow"), (4, "green")))
//    val join  = rdd1.join(rdd2)
//    join.foreach(println)
//    val data = Array(1,2,3,4,5)
//    val rdd = sc.parallelize(data)
//    val filter = rdd.filter(x => x%2 != 0)
//    val count = filter.count()
//    println(count)

//    val data = sc.textFile("C:/Users/HP ELITEBOOK/Desktop/shahid.txt")
//    val rdd1 = data.flatMap(x => x.split("  "))
//    val rdd2 = rdd1.map(x => (x,1))
//    val rdd3 = rdd2.reduceByKey((x,y) => (x+y))
//    val result = rdd3.sortBy(x => x._2, false)
//    result.take(1).foreach(println)
//    scala.io.StdIn.readLine()

  }

}
