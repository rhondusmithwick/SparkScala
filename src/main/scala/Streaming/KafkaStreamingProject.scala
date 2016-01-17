package Streaming

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Problem statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment6.html (Part 2)
 * Uses Kafka to read and evaluate event Logs.
 * @author Rhondu Smithwick
 */
object KafkaStreamingProject extends StreamingProject with App {

  // Set up writing to Kafka.
  val producer = createProducer()

  // Begin the  events stream.
  val dStreamEvents = ssc.textFileStream(logsDir)

  // Begin the Kafka Stream.
  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "localhost:9092")
  val topicsSet = Set("eventLogging")
  val dStreamKafka = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)

  go()
  ssc.start()
  ssc.awaitTermination()

  /**
   * Work on the DataStreams from above.
   * @see #Streaming.StreamingProject.getResults()
   */
  def go(): Unit = {
    dStreamEvents.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        rdd.foreach(x => producer.send(new KeyedMessage[String, String]("eventLogging", x)))
      }
    }
    dStreamKafka.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val df = sqlContext.read.json(rdd.map(x => x._2))
        getResults(df).foreach { x =>
          println(x)
          producer.send(new KeyedMessage[String, String]("results", x))
        }
      }
    }
  }

  /**
   * Creates a producer to write to Kafka.
   * @return a Kafka producer
   */
  def createProducer(): Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val config = new ProducerConfig(props)
    new Producer[String, String](config)
  }


}
