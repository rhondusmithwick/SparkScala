package Programs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Problem Statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment1.html
 * Works on White-House visitor records to get people who have visited the most, been visited the most,
 * and the most frequent combination.
 * @author Rhondu Smithwick
 */
object WhiteHouse extends App {
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

  // Set up Spark.
  val conf = new SparkConf().setAppName("WhiteHouseNoDF")
    .setMaster("local")
  val sc = new SparkContext(conf)

  // Read in the data.
  val data = dataRead()

  // Get the results.
  mostVisitor()
  mostVisited()
  mostCombo()

  sc.stop()

  /**
   * Read and parse the files, create a rowRDD with relevant data, and then create a table.
   */
  def dataRead(): RDD[(String, String)] = {
    val csvInput = sc.textFile(inputDir)
      .map(_.split(",")
      .map(_.trim))
    val header = csvInput.first()
    csvInput.filter(x => !(x sameElements header))
      .map(x => header.zip(x).toMap)
      .map(x => (x("NAMELAST") + ", " + x("NAMEFIRST") + ", " + x("NAMEMID"),
      x("visitee_namelast") + ", " + x("visitee_namefirst")))
  }

  /**
   * Get the 10 most frequent visitors.
   */
  def mostVisitor(): Unit = {
    val mostVisitorAns = data.map(x => (x._1, 1))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering[Int]
      .reverse.on(x => x._2))
      .map(x => "visitorName: " + x._1 + " visited " + x._2 + " times")
    println("(i) 10 most frequent visitors:")
    mostVisitorAns.foreach(println)
  }

  /**
   * Get the 10 most frequent visitees.
   */
  def mostVisited(): Unit = {
    val mostVisitedAns = data.map(x => (x._2, 1))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering[Int]
      .reverse.on(x => x._2))
      .map(x => "visiteeName: " + x._1 + " was visited " + x._2 + " times")
    println("(ii) 10 most frequently visited:")
    mostVisitedAns.foreach(println)
  }

  /**
   * Get the 10 most frequent visited-vistee combos.
   */
  def mostCombo(): Unit = {
    val mostComboAns = data.map(x => (x._1 + " visited " + x._2, 1))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering[Int]
      .reverse.on(x => x._2))
      .map(x => "visitorName: " + x._1 + " visited visiteeName: " + x._2 + " times.")
    println("(iii) 10 most frequent combo:")
    mostComboAns.foreach(println)
  }
}
