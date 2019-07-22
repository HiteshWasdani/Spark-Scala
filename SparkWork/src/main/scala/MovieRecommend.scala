import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import org.apache.spark.mllib.recommendation._

object MovieRecommend
{

  def movieNameExtractor():Map[Int,String]=
  {

    val dataset = Source.fromFile("/home/hitesh/Desktop/movies.csv").getLines()

    var movieName:Map[Int,String] = Map()     // create a moviename that is of map type

    for(line <- dataset)
      {
        val fields = line.split(",")
        val mid = fields(0).toInt
        val name = fields(1)

        movieName += (mid -> name)          // appending data with movie id as key of map and name as value
      }

    return movieName
  }


  def main(args: Array[String]): Unit =
  {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("Loading movie names...")
    var movieName = movieNameExtractor()      // load data from dataset to moviename of type map

    var sc = new SparkContext("local[*]","myMovieRecommend")

    val ratingDataset = sc.textFile("/home/hitesh/Desktop/ratings.csv")
    val ratingrdd = ratingDataset.map(x => x.split(",")).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble))   // Here rating class will be used to train data model (user,product,rating)

    println("\nTraining recommendation model...")

    val rank = 8                    // generally 5-10
    val numIteration = 15           // it should me aroung 15-20 for better result

    val model = ALS.train(ratingrdd,rank,numIteration)

    val userId = 1

    println(s"\n\nprinting rated movie by $userId...\n\n")

    val userRatedMoive = ratingrdd.filter(x => x.user == userId)
    val userRatedMovieResult = userRatedMoive.collect()

    for(i <- userRatedMovieResult)
      {
        println(movieName(i.product.toInt)+ "   :  " + i.rating.toString)
      }

    println("\n\n\nprinitng recommend products...\n\n ")
    val recommendProduct = model.recommendProducts(userId,10)   // num is value of how many records are needed

    for(i <- recommendProduct)
      {
        println(movieName(i.product.toInt) + "  rating   " + i.rating.toString)
      }
  }

}
