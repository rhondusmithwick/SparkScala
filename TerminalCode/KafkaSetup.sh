# cd into kafka 
cd /Users/rhondusmithwick/Documents/SparkScalaSource/kafka_2.10-0.9.0.0;

# Start Zookeeper server. 
bin/zookeeper-server-start.sh config/zookeeper.properties;

# Start Kafka Server (new terminal in Kafka folder).
bin/kafka-server-start.sh config/server.properties;

# Set up Topics (new Terminal in kafka folder).
bin/kafka-topics.sh \
--create --zookeeper localhost:2181 \
--replication-factor 1 --partitions 1 \
--topic eventLogging &
bin/kafka-topics.sh \
--create --zookeeper localhost:2181 \
--replication-factor 1 --partitions 1 \
--topic results;

# Look at Topics. 
bin/kafka-topics.sh \
--list \
--zookeeper localhost:2181;

# Start producer. 
bin/kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic eventLogging;
bin/kafka-console-producer.sh \
--broker-list localhost:9092 \
--topic results;

# Start consumer (new window). 
bin/kafka-console-consumer.sh \
--zookeeper localhost:2181 \
--topic eventLogging;
bin/kafka-console-consumer.sh \
--zookeeper localhost:2181 \
--topic results;

