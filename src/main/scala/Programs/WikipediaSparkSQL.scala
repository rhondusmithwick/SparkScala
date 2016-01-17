package Programs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Problem Statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment1.html (Part A)
 * Works on Wikipedia dataset to get pages with no outlinks and pages with no inlinks.
 * @author Rhondu Smithwick
 */
object WikipediaSparkSQL extends App {
  // Disable screen info.
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  // AWS.
  /*
    val IP = "..."
    val SPARK_MASTER = "spark://" + IP + ":7077"
    val HDFS = "hdfs://" + IP + ":9000"
    val inputDir = HDFS + "/home/ec2-user/WikipediaData"
    val outputDir = HDFS + "/home/ec2-user/WikipediaOutput"
  */

  // Local.
  val inputDir = "/Users/rhondusmithwick/Documents/SparkScala/WikipediaData"
  //  val outputDir = "/Users/rhondusmithwick/Documents/SparkScala/WikipediaOutput"

  // Set up Spark.
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Wikipedia")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  /**
   * A case class the will represent the pages.
   * @param pageIndex the index of the page
   * @param page the page
   */
  case class Page(pageIndex: String, page: String)

  pagesCreate()

  /**
   * A case class the will represent the links for tables.
   * @param linkIndex the index of the link
   */
  case class Link(linkIndex: String)

  val linksRDD = sc.textFile(inputDir + "/links-simple-sortedtest.txt")
    .map(x => x.split(":")
    .map(_.trim))

  // Get pages with no outlinks.
  outLinks()
  // Get pages with no inlinks.
  inLinks()

  sc.stop()

  /**
   * Creates the pages table.
   */
  def pagesCreate(): Unit = {
    sc.textFile(inputDir + "/titles-sortedtest.txt")
      .map(_.trim).zipWithIndex()
      .map(x => Page((x._2 + 1).toString, x._1))
      .toDF()
      .registerTempTable("pages")
  }

  /**
   * Get the pages with no outlinks.
   */
  def outLinks(): Unit = {
    linksRDD.map(x => Link(x(0)))
      .toDF()
      .registerTempTable("hasoutlinks")
    val noOutLinks = sqlContext.sql("WITH subquery AS (SELECT * FROM pages LEFT JOIN hasoutlinks " +
      "ON pages.pageIndex = hasoutlinks.linkIndex)  SELECT page FROM subquery WHERE linkIndex IS NULL")
      .map(x => x(0))
    println("No Outlinks:")
    noOutLinks.foreach(println)
  }

  /**
   * Get pages with no inlinks.
   */
  def inLinks(): Unit = {
    linksRDD.flatMap(x => x(1).split(" "))
      .map(x => Link(x))
      .toDF()
      .registerTempTable("hasinlinks")
    val noInLinks = sqlContext.sql("WITH subquery AS (SELECT * FROM pages LEFT JOIN hasinlinks " +
      "ON pages.pageIndex = hasinlinks.linkIndex)  SELECT page FROM subquery WHERE linkIndex IS NULL")
      .map(x => x(0))
    println("No Inlinks:")
    noInLinks.foreach(println)
  }

}