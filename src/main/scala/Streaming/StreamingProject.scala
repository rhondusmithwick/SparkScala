package Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.HashSet

/**
  * Problem statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment6.html
  * @author Rhondu Smithwick
  */
trait StreamingProject {
   // Disable screen info.
   Logger.getLogger("org").setLevel(Level.ERROR)
   Logger.getLogger("akka").setLevel(Level.ERROR)

   // Location of the logs.
   val logsDir = "/Users/rhondusmithwick/Documents/SparkScala/eventLogging"

   /**
    * A case class for keeping track of the current average of the tasks.
    * @param theSum current sum
    * @param theNum current number of tasks
    * @param avg the average
    */
   case class AvgTaskTime(var theSum: Long, var theNum: Int, var avg: Double)

   // These variables will keep track of the results.
   val appsSoFar, eventsSoFar, tasksNotEnded: HashSet[String] = new HashSet()
   val currAvg = new AvgTaskTime(0, 0, 0)
   val greaterTasks: HashSet[(Long, String)] = new HashSet()

   // Set up spark and the input directory for the logs.
   val conf = new SparkConf()
     .setMaster("local")
     .setAppName("StreamingProject")
   val ssc = new StreamingContext(conf, Seconds(1))

   /**
    * Gets the results of the computations.
    * @param df dataframe created from the stream
    * @return a list of strings that are representations of the results
    * @see #numApps(Dataframe)
    * @see #numEvents(Dataframe)
    * @see #taskNotEnd(Dataframe)
    * @see #averageTime(Dataframe)
    * @see #greatTasks(Dataframe)
    */
   def getResults(df: DataFrame): List[String] = {
     numApps(df) :: numEvents(df) :: tasksNotEnd(df) :: averageTime(df) ::
       greatTasks(df) :: "#############" :: Nil
   }

   /**
    * Keeps track of the total number of Spark applications seen so far.
    * @param df dataframe from the stream
    * @return a string of how many applications have been seen so far.
    */
   def numApps(df: DataFrame): String = {
     if (df.columns contains "App ID") {
       if (df.columns contains "App Name") {
         df.select("App ID", "App Name")
           .filter("`App ID` is not null and `App Name` is not null")
           .map(x => "Unique ID was " + x(0).toString + " and AppName was " + x(1).toString)
           .collect()
           .foreach(appsSoFar.add)
         //    println("Applications seen so far: ")
         //    appsSoFar.foreach(println)
       }
     }
     "Number of applications seen so far: " + appsSoFar.size
   }

   /**
    * Keeps track of the total number of distinct types of events seen so far across all Spark applications.
    * @param df dataframe from the stream
    * @return a string of total number of distinct types of events seen so far
    */
   def numEvents(df: DataFrame): String = {
     df.select("Event")
       .map(x => x(0).toString)
       .collect()
       .foreach(eventsSoFar.add)
     //    println("Event Types seen so Far:")
     //    eventsSoFar.foreach(println)
     "Number of types of events seen so far: " + eventsSoFar.size
   }

   /**
    * Keeps track of the total number of tasks that have started so far across all Spark applications, but have not ended yet.
    * @param df dataframe from the stream
    * @return a string of how many tasks have not ended
    */
   def tasksNotEnd(df: DataFrame): String = {
     // does not work
     if (df.columns contains "Task Info") {
       df.select("Stage ID", "Task Info.Task ID", "Task Info.Launch Time", "Task Info.Finish Time")
         .filter("`Task ID` is not null")
         .map(x => (x(3), "Stage ID: " + x(0).toString
         + " Task ID: " + x(1).toString
         + " Launch Time " + x(2).toString))
         .collect()
         .foreach { x =>
         if (x._1 == 0) tasksNotEnded.add(x._2)
         else tasksNotEnded.remove(x._2)
       }
     }
     "Number of Tasks Not Ended: " + tasksNotEnded.size
   }

   /**
    * Keeps track of the average running time of all tasks seen so far.
    * @param df dataframe from the stream
    * @return a string of the average running time of all tasks seen so far
    */
   def averageTime(df: DataFrame): String = {
     if (df.columns contains "Task Metrics") {
       df.select("Task Metrics.Executor Run Time")
         .filter("`Executor Run Time` is not null")
         .map(x => x(0).toString.toLong)
         .collect()
         .foreach { x =>
         currAvg.theSum = currAvg.theSum + x
         currAvg.theNum += 1
       }
       currAvg.avg = currAvg.theSum / currAvg.theNum.toDouble
     }
     "Average Running Time: " + currAvg.avg
   }

   /**
    * Keeps track of all tasks seen so far whose running time is greater than the average task running time seen so far.
    * @param df dataframe from the stream
    * @return a string of how many greater tasks there are
    */
   def greatTasks(df: DataFrame): String = {
     if (df.columns contains "Task Info") {
       if (df.columns contains "Task Metrics") {
         df.select("Stage ID", "Task Info.Task ID", "Task Info.Launch Time", "Task Metrics.Executor Run Time")
           .filter("`Launch Time` is not null and `Executor Run Time` is not null")
           .map(x => (x(3).toString.toLong,
           "Stage ID: " + x(0).toString
             + " Task ID: " + x(1).toString
             + " Launch Time " + x(2).toString))
           .collect()
           .foreach(x => if (x._1 > currAvg.avg) greaterTasks.add(x))
         greaterTasks.foreach(x => if (x._1 < currAvg.avg) greaterTasks.remove(x))
       }
     }
     "Greater Than Average Tasks number: " + greaterTasks.size
   }
 }
