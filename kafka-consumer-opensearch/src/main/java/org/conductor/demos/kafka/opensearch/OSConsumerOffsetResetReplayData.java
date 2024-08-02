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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
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

// Data retention policy - when offset reset to be done - on consumer being down for some/long time
// demo to replay data
// add graceful shutdown and resume again from the committed offset while shutdown

public class OSConsumerOffsetResetReplayData {

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OSConsumerOffsetResetReplayData.class.getSimpleName());

        //first create OS Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        //subscribe consumer to topic
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        //get a ref to the main thread
        final Thread mainThread = Thread.currentThread();
        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a Shutdown, lets exit by calling consumer.wakeup()...");
                kafkaConsumer.wakeup();
                //join the mainThread to allow the execution of the code in the mainThread
                try {
                    mainThread.join();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }

            }
        });

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

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : consumerRecords) {

                    // Idempotent: Strategy 1 - define ID using kafka record coordinates
                    // String id =record.topic() + "_" + record.partition() + "_" + record.offset();

                    // If your data provides an ID use that (recommended)
                    // Idempotent: Strategy 2 - extract ID from JSON Value
                    String id = extractID(record.value());

                    // send record into OS
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

                    //very inefficient
                    //IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    //create a bulk request to process all polled records - add all indexRequest to the BulkRequest
                    bulkRequest.add(indexRequest);

                    // avoid overwhelming logs
                    // log.info("Inserted one document into open search!" + " indexResponseID: " + indexResponse.getId());

                }
                if (bulkRequest.numberOfActions() > 0) {
                    //process all index requests in bulk request
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted: " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }

                    // commit offsets only if we are doing bulk requests
                    kafkaConsumer.commitSync(); // at least ONCE strategy - committing only after processing a batch of messages consumed during poll()
                    log.info("Offsets have been committed");
                }
            }
            // TWR auto-close things
        } catch (WakeupException we) {
            log.info("OS Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected exception in the OS consumer");
        } finally {
            openSearchClient.close();
            kafkaConsumer.close(); //close the consumer & this will also commit the offset
            // kafka does some internal closure steps
            // rebalance occurs when new consumer joins the group and when new partition get added to topic
            log.info(" OS Consumer is now gracefully shutdown !!");
        }
    }

    private static String extractID(String json) {

        // gson library
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";
        String bootstrapServers = "127.0.0.1:9092";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
