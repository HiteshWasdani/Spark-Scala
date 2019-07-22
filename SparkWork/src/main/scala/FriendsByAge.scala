import org.apache.log4j._
import org.apache.spark._

object FriendsByAge{

  def lineParser(line : String) =
  {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numfriends = fields(3).toInt
    (age,numfriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendByAge")
    val data = sc.textFile("/home/hitesh/Desktop/file.csv")

    val rdd = data.map(lineParser)

    val mapped_value = rdd.mapValues(x => (x,1))
    val reduced_value = mapped_value.reduceByKey((x,y) => (x._1 + y._1, x._2+ y._2))

    val avg = reduced_value.mapValues( x => x._1/x._2)
    val result = avg.collect()
    result.sorted.foreach(println)
  }
}