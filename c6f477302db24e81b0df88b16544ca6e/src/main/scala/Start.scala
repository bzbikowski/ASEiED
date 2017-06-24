import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable

class Start {
  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark project")
    .getOrCreate()

  def getData() : Unit = {
    val sc = sparkSession.sparkContext
    val file = "./src/main/resources/dataMay-31-2017.json"
    val parsed = sc.wholeTextFiles(file).values
    val data = sparkSession.read.json(parsed)
    val points = data.withColumn("data", explode(data.col("data"))).select("data")
    //-----------------------------------------------------------------------------
    val point_team1 = new Array[Point](points.count().asInstanceOf[Int])
    val point_team2 = new Array[Point](points.count().asInstanceOf[Int])
    val list = points.collectAsList()
    for(i <- 0 until list.size()){
      val temp = list.get(i).apply(0).asInstanceOf[mutable.WrappedArray[Long]]
      point_team1.update(i, new Point(temp.apply(0), temp.apply(1)))
      point_team2.update(i, new Point(temp.apply(2), temp.apply(3)))
    }
    //-----------------------------------------------------------------------------
    val team1_apriori = point_team1.length.toFloat / (2 * point_team1.length)
    val team2_apriori = point_team2.length.toFloat / (2 * point_team2.length)
    val target = new Point(30, 30) // test point
    val radius = 20
    val team1_par = sc.parallelize(point_team1)
    val team2_par = sc.parallelize(point_team2)
    val inrange1 = team1_par.filter(v => Math.sqrt(Math.pow((v.x - target.x).asInstanceOf[Double], 2) + Math.pow((v.y - target.y).asInstanceOf[Double], 2)) < radius)
    val inrange2 = team2_par.filter(v => Math.sqrt(Math.pow((v.x - target.x).asInstanceOf[Double], 2) + Math.pow((v.y - target.y).asInstanceOf[Double], 2)) < radius)
    val team1_postpriori = inrange1.count() / point_team1.length.toFloat
    val team2_postpriori = inrange2.count() / point_team2.length.toFloat
    val prawd_team1 = team1_apriori * team1_postpriori
    val prawd_team2 = team2_apriori * team2_postpriori
    println("Prawdopodobienstwo nr 1 : " + prawd_team1 * 100 + " %")
    println("Prawdopodobienstwo nr 2 : " + prawd_team2 * 100 + " %")
  }
}
