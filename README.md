# Kafka Twitter Example

This application is divided into two modules. The first module is called "producer" reads tweets from Twitter and sends
them to a Kafka topic. The second module is called "consumer" and reads the tweets from the Kafka topic and writes them
to an Elasticsearch index. The infrastructure required for running the application can be created with Docker Compose
by running the command `docker-compose up -d` in the root folder of the project. In total, this will create four Docker
containers: one container for running a Zookeeper instance, one container for running a Kafka broker, one container for
running an Elasticsearch node and one container for running a Kibana instance. The Docker containers can be stopped and
removed by running the command `docker-compose down`. But this command will not delete the Docker volumes that were
created for storing the static of the Zookeeper instance, the Kafka broker and the Elasticsearch node. The Kafka topic,
the Elasticsearch index and all other data can be removed by deleting the corresponding Docker volumes. This can be
done by running the command `docker-compose down -v`.

When the producer module is started, it will check if the Kafka topic `tweets` has already been created. If the topic
does not exist yet, the producer will create the topic. Similary, when the consumer is started, it will check if the
Elasticsearch index `tweets` has already been created. If the index does not exists yet, the consumer will create the
index. The index can be accessed by using Kibana. The Kibana instance that has been started as a Docker container can
be accessed [here](http://localhost:5601).

The application also requires a Twitter developer account that can be created [here](https://developer.twitter.com).
After creating a Twitter developer account, a new Twitter app needs to be created. The credentials of that app have to
be the assigned to the corresponding static fields of the producer. The producer also contains a field called `TERMS`.
This field contains the list of terms that will be tracked by the Twitter client.

###### Useful commands

* `docker logs zookeeper` (displays the logs of of the Zookeeper instance)
* `docker logs kafka` (displays the logs of of the Kafka broker)
* `docker run --net=host --rm confluentinc/cp-kafka:5.0.0 kafka-topics --zookeeper localhost:2181 --describe --topic tweets`
(describes the Kafka topic that has been created by the producer)
* `docker run --net=host --rm confluentinc/cp-kafka:5.0.0 kafka-consumer-groups --bootstrap-server localhost:9092 --group kafka-twitter-example --describe --all-topics`
(describes the Kafka consumer group that is used by the consumer)