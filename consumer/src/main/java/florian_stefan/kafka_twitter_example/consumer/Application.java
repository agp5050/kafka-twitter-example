package florian_stefan.kafka_twitter_example.consumer;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  private static final String TOPIC = "tweets";
  private static final String INDEX = "tweets";
  private static final String TYPE = "_doc";

  private final CountDownLatch countDownLatch;
  private final KafkaConsumer<String, String> kafkaConsumer;
  private final RestHighLevelClient restHighLevelClient;

  private Thread thread;

  Application() {
    countDownLatch = new CountDownLatch(1);
    kafkaConsumer = createKafkaConsumer();
    restHighLevelClient = createRestHighLevelClient();

    createIndexIfNecessary();

    Runtime.getRuntime().addShutdownHook(new Thread(this::stopAndAwaitShutdown));
  }

  Application start() {
    synchronized (this) {
      if (thread == null) {
        thread = new Thread(this::run);
        thread.start();
      } else {
        LOG.warn("Application has already been started.");
      }
    }

    return this;
  }

  void awaitShutdown() {
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LOG.error("An error occurred while awaiting shutdown.", e);
    }
  }

  private KafkaConsumer<String, String> createKafkaConsumer() {
    Properties properties = new Properties();

    addBasicSettings(properties);
    addGroupSettings(properties);
    addBatchSettings(properties);

    return new KafkaConsumer<>(properties);
  }

  private void addBasicSettings(Properties properties) {
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  private void addGroupSettings(Properties properties) {
    properties.setProperty(GROUP_ID_CONFIG, "kafka-twitter-example");
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
  }

  private void addBatchSettings(Properties properties) {
    properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(MAX_POLL_RECORDS_CONFIG, "200");
  }

  private RestHighLevelClient createRestHighLevelClient() {
    return new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
  }

  private void createIndexIfNecessary() {
    try {
      if (restHighLevelClient.indices().exists(new GetIndexRequest().indices(INDEX))) {
        LOG.info("Index {} has already been created.", INDEX);
      } else {
        LOG.info("Starting to create index {}.", INDEX);
        restHighLevelClient.indices().create(new CreateIndexRequest(INDEX));
        LOG.info("Successfully created index {}.", INDEX);
      }
    } catch (IOException e) {
      LOG.error("An error occurred while checking or creating index.", e);
    }
  }

  private void stopAndAwaitShutdown() {
    kafkaConsumer.wakeup();
    awaitShutdown();
  }

  private void run() {
    kafkaConsumer.subscribe(singleton(TOPIC));

    try {
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(200));

        processRecords(records);

        kafkaConsumer.commitSync();

        waitBeforeNextPoll();
      }
    } catch (WakeupException e) {
      LOG.info("Received shutdown signal.");
    } catch (Exception e) {
      LOG.error("An error occurred while processing records.", e);
    } finally {
      shutdown();
    }
  }

  private void processRecords(ConsumerRecords<String, String> records) throws IOException {
    LOG.info("Starting to process {} records.", records.count());

    BulkRequest bulkRequest = buildBulkRequest(records);

    if (hasIndexRequests(bulkRequest)) {
      restHighLevelClient.bulk(bulkRequest);
    }

    LOG.info("Successfully processed {} records.", records.count());
  }

  private BulkRequest buildBulkRequest(ConsumerRecords<String, String> records) {
    BulkRequest bulkRequest = new BulkRequest();

    for (ConsumerRecord<String, String> record : records) {
      String tweetAsJsonString = record.value();

      extractId(tweetAsJsonString).map(buildIndexRequest(tweetAsJsonString)).ifPresent(bulkRequest::add);
    }

    return bulkRequest;
  }

  private Optional<String> extractId(String tweetAsJsonString) {
    JsonObject tweetAsJsonObject = new JsonParser().parse(tweetAsJsonString).getAsJsonObject();

    if (tweetAsJsonObject.has("id_str")) {
      return Optional.of(tweetAsJsonObject.get("id_str").getAsString());
    }

    return Optional.empty();
  }

  private Function<String, IndexRequest> buildIndexRequest(String tweetAsJsonString) {
    return id -> new IndexRequest(INDEX, TYPE, id).source(tweetAsJsonString, JSON);
  }

  private boolean hasIndexRequests(BulkRequest request) {
    return !request.requests().isEmpty();
  }

  private void waitBeforeNextPoll() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.error("An error occurred while waiting before next poll.", e);
    }
  }

  private void shutdown() {
    try {
      LOG.info("Starting to close KafkaConsumer.");
      kafkaConsumer.close();
      LOG.info("Successfully closed KafkaConsumer.");
    } catch (Exception e) {
      LOG.error("An error occurred while closing KafkaConsumer!");
    }

    try {
      LOG.info("Starting to stop Elasticsearch client.");
      restHighLevelClient.close();
      LOG.info("Successfully stopped Elasticsearch client.");
    } catch (Exception e) {
      LOG.error("An error occurred while stopping Elasticsearch client!");
    }

    countDownLatch.countDown();
  }

  public static void main(String[] args) {
    new Application().start().awaitShutdown();
  }

}
