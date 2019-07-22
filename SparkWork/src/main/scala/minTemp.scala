import org.apache.log4j._
import org.apache.spark._
import scala.math.min

object minTemp {

  def lineParser(line : String) =
  {
    val fields = line.split(",")
    val stationId = fields(0)
    val info = fields(2)
    val temp = fields(3)
    (stationId,info, temp)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","minTemp")
    val data = sc.textFile("/home/hitesh/Desktop/minTemp")

    val rdd = data.map(lineParser)

    val filter_rdd = rdd.filter(x => x._2 =="TMIN")
    val newData = filter_rdd.map(x => (x._1, x._3.toFloat))
    val res1 = newData.reduceByKey((x,y) => min(x,y))

    val result = res1.collect()
    result.sorted.foreach(println)

  }
}
