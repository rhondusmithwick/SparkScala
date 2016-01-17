package Streaming

import org.apache.spark.sql.SQLContext

/**
 * Problem statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment6.html (Part 1)
 * Uses Spark Streaming to run operations on event logs.
 * @see Streaming.StreamingProject
 * @author Rhondu Smithwick
 */
object SparkStreamingProject extends StreamingProject with App {
  // Begin the events stream.
  val dStreamEvents = ssc.textFileStream(logsDir)

  go()
  ssc.start()
  ssc.awaitTermination()

  /**
   * Work on the dataStream using spark streaming.
   * @see #Streaming.StreamingProject.getResults()
   */
  def go(): Unit = {
    dStreamEvents.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val df = sqlContext.read.json(rdd)
        getResults(df).foreach(println)
      }
    }
  }
}
