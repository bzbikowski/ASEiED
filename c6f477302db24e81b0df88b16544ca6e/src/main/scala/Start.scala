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
    values_array.foreach{ value => array.update(i, Point(value.apply(0),value.apply(1),value.apply(2),value.apply(3)))
                          i += 1;}
    array.foreach{ println }
  }
}
