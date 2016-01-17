Original Problem statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment6.html
This package is a project on data streaming using Spark.

Streaming.StreamingProject.scala keeps track of the following using Spark event logs:
    1) Total number of Spark applications seen so far.
    2) Total number of distinct types of events seen so far across all Spark applications.
    3) Total number of tasks that have started so far across all Spark applications, but have not ended yet.
    4) Average running time of all tasks seen so far across all Spark applications.
    5) All tasks seen so far whose running time is greater than the average task running time seen so far.

SparkStreamingProject.scala works directly on a text file stream.

KafkaStreamingProject first sends the event logs to Kafka, then reads in the event logs from Kafka,
and finally sends the results to Kafka.

To run SparkStreamingProject.scala:
    1) Run SparkStreaming.scala
    2) Run TerminalCode/RunProgramsScript.sh
To run KafkaStreamingProject.scala:
    1) Set up the Zookeeper Server, Kafka Server, and topics (see TerminalCode/KafkaSetup.sh)
    2) Run KafkaStreamingProject.scala
    3) Run TerminalCode/RunPrograms.Script.sh
