import org.apache.log4j._
import org.apache.spark._

object CustomerWithHighestShopping {

  def lineParser(line :String) =
  {
    val fields = line.split(",")
    val cid = fields(0)
    val amt = fields(2).toFloat
    (cid,amt)
  }


  def main(args: Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Customer")
    val data = sc.textFile("/home/hitesh/Desktop/cust")

    val rdd = data.map(lineParser)

    val reduced_rdd = rdd.reduceByKey((x,y) => (x+y))
    val result = reduced_rdd.collect().sortBy(_._2)(Ordering.Float.reverse)  // these lines are used for desceing 

//    print(result.head)   // it is for only topmost value

    result.foreach(println)
  }
}