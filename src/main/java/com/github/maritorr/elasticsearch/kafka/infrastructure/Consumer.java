package com.github.maritorr.elasticsearch.kafka.infrastructure;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.maritorr.elasticsearch.infrastructure.BasicElasticClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private Properties properties;
    private KafkaConsumer consumer;
    private ObjectMapper objectMapper;
    private BasicElasticClient bec;

    public Consumer() {
        try {
            setUp();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void setUp() throws IOException {
        bec = new BasicElasticClient();
        objectMapper = new ObjectMapper();
        properties = new Properties();
        properties.load(Consumer.class.getResourceAsStream("/kafka-consumer.properties"));

        consumer = new KafkaConsumer(properties);
    }

    private void shutdown() {
        consumer.wakeup();
    }

    private void run() {
        CountDownLatch latch = new CountDownLatch(1);

        consumer.subscribe(Collections.singleton(properties.getProperty("twitter.producer.topic")));

        new Thread( () -> {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        // Send to elastic search
                        String twit = objectMapper.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(
                                        objectMapper.readValue(record.value(), Object.class)
                                );
                        //LOG.info("Twit \n ********** START ********** \n {} \n ********** END **********",
                        //        twit);

                        bec.index(UUID.randomUUID().toString(), twit);
                    }
                }
            } catch (WakeupException we) {
                LOG.info("Consumer will wake up...");
            } catch (JsonParseException e) {
                e.printStackTrace();
                LOG.error("Could not prettify JSON string", e);
            } catch (JsonMappingException e) {
                e.printStackTrace();
                LOG.error("Could not prettify JSON string", e);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                LOG.error("Could not prettify JSON string", e);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                latch.countDown();
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            this.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            LOG.info("Consumer did shutdown");

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            LOG.info("Consumer will shutdown...");
        }
    }

    public static void main(String[] args) {
        new Consumer().run();
    }
}
