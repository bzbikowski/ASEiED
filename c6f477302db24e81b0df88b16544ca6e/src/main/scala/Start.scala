import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable

import java.awt._
import java.awt.geom._
import java.awt.image.BufferedImage
import javax.swing.{ImageIcon, JOptionPane}

import scala.collection.mutable.ArrayBuffer

class Start {
  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark project")
    .getOrCreate()

  def getData() : Unit = {
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
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
      point_team1.update(i, new Point(temp.apply(0), temp.apply(1), Color.GREEN))
      point_team2.update(i, new Point(temp.apply(2), temp.apply(3), Color.RED))
    }
    //-----------------------------------------------------------------------------
    val team1_apriori = point_team1.length.toFloat / (2 * point_team1.length)
    val team2_apriori = point_team2.length.toFloat / (2 * point_team2.length)
    val target = new Point(40, 30, Color.BLACK) // test point
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
    if (prawd_team1 >= prawd_team2) {
      target.color = Color.GREEN;
    } else {
      target.color = Color.RED;
    }
    val basePoints = point_team1 ++ point_team2
    drawPoints(basePoints, target)
    showResult
  }

  def drawPoints(points: Array[Point], added: Point): Unit = {
    // Size of image
    val size = (550, 550)
    val factor = 10
    // create an image
    val canvas = new BufferedImage(size._1, size._2, BufferedImage.TYPE_INT_RGB)

    // get Graphics2D for the image
    val g = canvas.createGraphics()

    // clear background
    g.setColor(Color.WHITE)
    g.fillRect(0, 0, canvas.getWidth, canvas.getHeight)

    //draw all points
    for (i <- points.indices) {
      g.setColor(points(i).color)
      g.fill(new Ellipse2D.Double(points(i).x * factor - 2, points(i).y * factor - 2, 4.0, 4.0))
    }

    val FIELDSIZE = 20
    g.setColor(added.color)
    g.fill(new Ellipse2D.Double(added.x * factor - 6, added.y * factor - 6, 12.0, 12.0))
    g.draw(new Ellipse2D.Double(added.x * factor - FIELDSIZE * factor, added.y * factor - FIELDSIZE * factor, FIELDSIZE * factor * 2, 2 * FIELDSIZE * factor))

    javax.imageio.ImageIO.write(canvas, "png", new java.io.File("results.png"))
  }

  def showResult: Unit = {
    import java.awt.BorderLayout
    import javax.swing.{JFrame, JLabel}
    JFrame.setDefaultLookAndFeelDecorated(true)

    val frame: JFrame = new JFrame
    frame.setLayout(new BorderLayout)
    frame.setTitle("Bayes Classification Results")
    val img = new ImageIcon("results.png")

    val label: JLabel = new JLabel(img)
    frame.add(label)
    frame.pack()
    frame.setVisible(true)
    JOptionPane.showMessageDialog(frame, "Press \'OK\' to close program")
  }
}
