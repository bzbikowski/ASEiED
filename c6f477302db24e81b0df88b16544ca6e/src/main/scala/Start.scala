import org.apache.spark.sql.SparkSession


class Start {
  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark project")
    .getOrCreate()

  def getData : Unit = {
    val data = sparkSession.read.json("./src/main/resources/dataMay-31-2017.json")
    data.show()
  }
}
