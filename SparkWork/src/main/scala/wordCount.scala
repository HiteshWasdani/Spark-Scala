import org.apache.log4j._
import org.apache.spark._

object wordCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","wordCount")
    val data = sc.textFile("/home/hitesh/Desktop/book")
    sc.addJar("/home/hitesh/Desktop/SparkWork")

    val words = data.flatMap(x => x.split("\\W+"))
    val mappedWords = words.map(x => (x.toLowerCase(),1))

    val reducedWords = mappedWords.reduceByKey((x,y) => (x+y))
    val result = reducedWords.collect()

    result.sorted.foreach(println)


      }

}
