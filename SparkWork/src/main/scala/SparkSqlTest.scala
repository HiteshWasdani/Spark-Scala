import org.apache.spark.sql._
import org.apache.log4j._

object SparkSqlTest {

  case class Person(id:Int, name:String, age:Int, numFriends:Int)

  def mapper(line: String) =
  {
    val fields = line.split(",")
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    ( person)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TestSql").master("local[*]").getOrCreate()

    val lines = spark.sparkContext.textFile("/home/hitesh/Desktop/file.csv")
    val people = lines.map(mapper)

    import spark.implicits._

    val schemaPeople = people.toDF

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people1")

    val teenagers = spark.sql("SELECT * FROM people1 WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }

}
