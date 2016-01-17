package Programs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Problem Statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment1.html
 * Works on WhiteHouse visitor records to get people who have visited the most, been visited the most,
 * and the most frequent combination.
 * This version uses SparkSQL.
 * @author Rhondu Smithwick
 */
object WhiteHouseSparkSQL extends App {
  // Configue logs.
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  // AWS.
  /*
    val IP = "..."
    val SPARK_MASTER = "spark://" + IP + ":7077"
    val HDFS = "hdfs://" + IP + ":9000"
    val inputDir = HDFS + "/home/ec2-user/WhiteHouseData"
    val outputDir = HDFS + "/home/ec2-user/WhiteHouseaOutput"
  */

  // Local.
  val inputDir = "/Users/rhondusmithwick/Documents/SparkScala/WhiteHouseData/test.csv"
  //  val outputDir = "/Users/rhondusmithwick/Documents/SparkScala/WhiteHouseOutput"

  // Set up spark.
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("WhiteHouse")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  /**
   * A case class to create the table.
   * @param visitorName the visitor's Name
   * @param visiteeName the visitee's Name
   */
  case class Visit(visitorName: String, visiteeName: String)

  // Read in the Data and create the table.
  dataRead()
  // Get the results.
  mostVisitor()
  mostVisited()
  mostCombo()

  sc.stop()

  /**
   * Read and parse the files, create a rowRDD with relevant data, and then create a table.
   */
  def dataRead(): Unit = {
    val csvInput = sc.textFile(inputDir)
      .map(_.split(",")
      .map(_.trim))
    val header = csvInput.first()
    csvInput.filter(x => !(x sameElements header))
      .map(x => header.zip(x).toMap)
      .map(x => Visit(x("NAMELAST") + ", " + x("NAMEFIRST") + ", " + x("NAMEMID"),
      x("visitee_namelast") + ", " + x("visitee_namefirst")))
      .toDF()
      .registerTempTable("visits")
  }

  /**
   * Get the 10 most frequent visitors.
   */
  def mostVisitor(): Unit = {
    val mostVisitorAns = sqlContext.sql("SELECT visitorName, count(*) c FROM visits GROUP BY visitorName ORDER BY c DESC LIMIT 10")
      .collect()
      .map(x => "visitorName: " + x(0) + " visited " + x(1) + " times")
    println("(i) 10 most frequent visitors:")
    mostVisitorAns.foreach(println)
  }

  /**
   * Get the 10 most frequent visitees.
   */
  def mostVisited(): Unit = {
    val mostVisitedAns = sqlContext.sql("SELECT visiteeName, count(*) c FROM visits GROUP BY visiteeName ORDER BY c DESC LIMIT 10")
      .collect()
      .map(x => "visiteeName: " + x(0) + " was visited " + x(1) + " times")
    println("(ii) 10 most frequently visited:")
    mostVisitedAns.foreach(println)
  }

  /**
   * Get the 10 most frequent visited-vistee combos.
   */
  def mostCombo(): Unit = {
    val mostComboAns = sqlContext.sql("SELECT visitorName, visiteeName, count(*) c FROM visits GROUP BY visitorName, visiteeName ORDER BY c DESC LIMIT 10")
      .collect()
      .map(x => "visitorName: " + x(0) + " visited visiteeName: " + x(1) + " " + x(2) + " times")
    println("(iii) 10 most frequent combo:")
    mostComboAns.foreach(println)
  }

}