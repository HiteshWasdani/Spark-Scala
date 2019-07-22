import org.apache.log4j._
import org.apache.spark._

// Dont know what to do  so levaing it,, ouptut of its is cpation marvel 2/mo
object TopFamousHero {


  def extractName(line : String) =
  {
    val line1 = line.replaceAll("\"","")
    val fields = line1.split("\\s+")

    val hero_id = fields(0).trim
    var hero_name = ""

    for( i <- 1 to fields.length-1) hero_name = hero_name + " " +fields(i)

    (hero_id.toInt,hero_name)
  }

  def occurance(line :String) =
  {
    val fields = line.split("\\s+")
    val hero_id = fields(0)
    val num_associate = fields.length-1

    (hero_id,num_associate.toInt)
  }

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","TopFamousHero")
    val marvel_graph = sc.textFile("/home/hitesh/Desktop/Marvel-graph")
    val marvel_name = sc.textFile("/home/hitesh/Desktop/Marvel-names.txt")

    val name_rdd = marvel_name.map(extractName)
    val graph_rdd = marvel_graph.map(occurance)

//    val reduced_rdd = graph_rdd.reduceByKey((x,y) => (x+y))

    val result = graph_rdd.collect().sortBy(_._2)(Ordering.Int.reverse)

    val temp = result.head
    val topHero = name_rdd.lookup((temp._1).toInt)(0)

//    name_rdd.collect().sorted.foreach(println)
    print(topHero)
  }
}
