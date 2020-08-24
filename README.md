# Sample Spark project with a test
Code test a spark code

#Sample kafka with spark streaming test

## Setup a cluster 
1. Run your cluster: ```MY_IP=$(ipconfig getifaddr en0) docker-compose up```
2. Create a topic _test_: ```docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic test```
3. List all topics: ```docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper:2181```

##Test cluster
1. Install kafkacat: ```brew install kafkacat```
2. Check if you can get metadata: ```kafkacat -b localhost:9093 -L```
2. Listen on partition 1: ```kafkacat -C -b localhost:9093 -t test -p 1```
3. Write to partition 1: ```echo 'hello partition 1' | kafkacat -P -b localhost:9093 -t test -p 1```
4. Repeat the same commands for Partitons 2,3 and 4
