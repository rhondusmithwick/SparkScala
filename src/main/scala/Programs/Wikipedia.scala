package Programs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Problem Statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment1.html (Part A)
  * Works on Wikipedia dataset to get pages with no outlinks and pages with no inlinks.
  * @author Rhondu Smithwick
  */
object Wikipedia extends App {
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


   val pages= pagesCreate()

   val links = linksCreate()

   // Get pages with no outlinks.
   outLinks()
   // Get pages with no inlinks.
   inLinks()

   sc.stop()

   /**
    * Creates the pagesRDD.
    * @return an RDD of the pages where the RDD is of tuples; the first element of the tuple is the index of the page
    *         and the second is the page
    */
   def pagesCreate(): RDD[(String, String)] = {
     sc.textFile(inputDir + "/titles-sortedtest.txt")
       .map(_.trim)
       .zipWithIndex()
       .map(x => ((x._2 + 1).toString, x._1))
   }

  /**
   * Create the links RDD.
   * @return an RDD of the links; the first element is the page and the rest are the page it links to.
   */
  def linksCreate(): RDD[Array[String]] = {
   sc.textFile(inputDir + "/links-simple-sortedtest.txt")
      .map(x => x.split(":")
      .map(_.trim))
  }

   /**
    * Get the pages with no outlinks.
    */
   def outLinks(): Unit = {
     val hasOutLinks = links.map(x => (x(0),"PlaceHolder"))
     val noOutLinks = pages.subtractByKey(hasOutLinks)
      .map(x => x._1)
     println("No Outlinks:")
     noOutLinks.foreach(println)
   }

   /**
    * Get pages with no inlinks.
    */
   def inLinks(): Unit = {
     val hasInLinks= links.flatMap(_(1).split(" ").map(x=> (x, "PlaceHolder")))
     val noInLinks = pages.subtractByKey(hasInLinks)
      .map(x => x._1)
     println("No Inlinks:")
     noInLinks.foreach(println)
   }

 }