package florian_stefan.kafka_twitter_example.producer;

import static com.google.common.collect.Lists.newArrayList;
import static com.twitter.hbc.core.Constants.STREAM_HOST;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  private static final String CONSUMER_KEY = "";
  private static final String CONSUMER_SECRET = "";
  private static final String ACCESS_TOKEN = "";
  private static final String ACCESS_TOKEN_SECRET = "";

  private static final List<String> TERMS = newArrayList("twitter", "kafka", "elasticsearch");
  private static final String TOPIC = "tweets";

  private final BlockingQueue<String> messageQueue;
  private final Client twitterClient;
  private final KafkaProducer<String, String> kafkaProducer;

  Application() {
    messageQueue = new LinkedBlockingQueue<>(1000);
    twitterClient = createTwitterClient(messageQueue);
    kafkaProducer = createKafkaProducer();

    createTopicIfNecessary();

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  void run() {
    twitterClient.connect();

    while (!twitterClient.isDone()) {
      String message = null;

      try {
        message = messageQueue.poll(5, SECONDS);
      } catch (InterruptedException e) {
        LOG.error("An error occurred while polling for messages.", e);

        twitterClient.stop();
      }

      if (message != null) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, null, message));
      }
    }
  }

  private Client createTwitterClient(BlockingQueue<String> messageQueue) {
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    endpoint.trackTerms(TERMS);

    ClientBuilder builder = new ClientBuilder()
        .hosts(new HttpHosts(STREAM_HOST))
        .authentication(new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET))
        .endpoint(endpoint)
        .processor(new StringDelimitedProcessor(messageQueue));

    return builder.build();
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties = new Properties();

    addBasicSettings(properties);
    addRetrySettings(properties);
    addBatchSettings(properties);

    return new KafkaProducer<>(properties);
  }

  private void addBasicSettings(Properties properties) {
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }

  private void addRetrySettings(Properties properties) {
    properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ACKS_CONFIG, "all");
    properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
  }

  private void addBatchSettings(Properties properties) {
    properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(LINGER_MS_CONFIG, "20");
    properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
  }

  private void createTopicIfNecessary() {
    Properties properties = new Properties();

    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    try (AdminClient adminClient = KafkaAdminClient.create(properties)) {
      if (adminClient.listTopics().names().get().contains(TOPIC)) {
        LOG.info("Topic {} has already been created.", TOPIC);
      } else {
        LOG.info("Starting to create topic {}.", TOPIC);
        adminClient.createTopics(singleton(new NewTopic(TOPIC, 5, (short) 1))).all().get();
        LOG.info("Successfully created topic {}.", TOPIC);
      }
    } catch (Exception e) {
      LOG.error("An error occurred while checking or creating topic.", e);
    }
  }

  private void shutdown() {
    LOG.info("Received shutdown signal.");

    try {
      LOG.info("Starting to stop Twitter client.");
      twitterClient.stop();
      LOG.info("Successfully stopped Twitter client.");
    } catch (Exception e) {
      LOG.error("An error occurred while stopping Twitter client!");
    }

    try {
      LOG.info("Starting to close KafkaProducer.");
      kafkaProducer.close();
      LOG.info("Successfully closed KafkaProducer.");
    } catch (Exception e) {
      LOG.error("An error occurred while closing KafkaProducer!");
    }

    LOG.info("Successfully shutdown application.");
  }

  public static void main(String[] args) {
    new Application().run();
  }

}
