package org.conductor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// committing offset - manually - to ensure the below strategy
// at least ONCE stratergy - committing only after processing a batch of messages consumed during poll()

public class OSConsumerDeliverySemantics {

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OSConsumerDeliverySemantics.class.getSimpleName());

        //first create OS Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        //subscribe consumer to topic
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        //we need to create index on OS if it doesn't exist already
        try (openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created");
            } else {
                log.info("The wikimedia index already exists");
            }

            //main code logic
            while (true) {

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(3000)); //3ms
                int recordCount = consumerRecords.count();
                log.info("Received: " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record : consumerRecords) {

                    //Idempotent: Strategy 1 - define ID using kafka record coordinates
                    // String id =record.topic() + "_" + record.partition() + "_" + record.offset();

                    //If your data provides an ID use that (recommended)
                    //Idempotent: Strategy 2 - extract ID from JSON Value
                    String id = extractID(record.value());

                    //send record into OS
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    //log.info("Inserted one document into open search!" + " indexResponseID: " + indexResponse.getId());

                }
                // commit offsets after the whole batch is consumed
                kafkaConsumer.commitSync(); // at least ONCE stratergy - committing only after processing a batch of messages consumed during poll()
                log.info("Offsets have been committed");
            }
            //TWR auto-close things
        }
    }

    private static String extractID(String json) {

        //gson library
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";
        String bootsrapServers = "127.0.0.1:9092";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootsrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://127.0.0.1:9200"; //localhost:9200
        // String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
