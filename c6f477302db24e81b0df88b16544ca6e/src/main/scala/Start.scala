import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class Start {
  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark project")
    .getOrCreate()

  def getData : Unit = {
    val data = sparkSession.read.json("./src/main/resources/dataMay-31-2017.json")
    //val keys_array = data.first().get(0).asInstanceOf[mutable.WrappedArray[String]]
    val values_array = data.first().get(1).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Long]]]
    val array = new Array[Point](values_array.length)
    var i = 0
    values_array.foreach { value =>
      array.update(i, Point(value.apply(0), value.apply(1), value.apply(2), value.apply(3)))
      i += 1;
    }
    val team1_apriori = array.length.toFloat / (2 * array.length)
    val team2_apriori = array.length.toFloat / (2 * array.length)
    val target = Target_point(30, 30)
    val radius = 15
    var team1 = 0
    var team2 = 0
    array.foreach { item => if(check_blue(target, radius, item)) team1 += 1}
    array.foreach { item => if(check_red(target, radius, item)) team2 += 1}
    val team1_postpriori = team1 / array.length.toFloat
    val team2_postpriori = team2 / array.length.toFloat
    val prawd_team1 = team1_apriori * team1_postpriori
    val prawd_team2 = team2_apriori * team2_postpriori
    println("Prawdopodobienstwo nr 1 : " + prawd_team1 * 100 + " %")
    println("Prawdopodobienstwo nr 2 : " + prawd_team2 * 100 + " %")
  }

  def check_blue (target:Target_point, radius:Int, item:Point) : Boolean = {
    val punkt = Target_point(item.x1, item.y1)
    if(Math.sqrt(pow(punkt.x - target.x) + pow(punkt.y - target.y)) < radius)
      return true
    else
      return false
  }

  def check_red (target:Target_point, radius:Int, item:Point) : Boolean = {
    val punkt = Target_point(item.x2, item.y2)
    if(Math.sqrt(pow(punkt.x - target.x) + pow(punkt.y - target.y)) < radius)
      return true
    else
      return false
  }

  def pow(l : Long) : Long = {
    return l * l
  }
}
