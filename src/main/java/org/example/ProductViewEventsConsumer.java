package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.entities.ProductEventAggregated;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ProductViewEventsConsumer {

    private final String CONSUMER_GROUP_ID;
    private final ObjectMapper mapper;
    private final String kafkaServer;
    private final String sourceTopic;
    private final Jedis redisConnection;

    public ProductViewEventsConsumer(
        String kafkaServer,
        String sourceTopic,
        Jedis redisConnection
    ) {
        this.CONSUMER_GROUP_ID = "product-events-aggregation-consumer-group";
        this.mapper = new ObjectMapper();
        this.kafkaServer = kafkaServer;
        this.sourceTopic = sourceTopic;
        this.redisConnection = redisConnection;
    }

    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProducerProps());

        try (consumer) {
            consumer.subscribe(Collections.singletonList(sourceTopic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    ProductEventAggregated productEventAggregated = serializeMessage(record.value());

                    if (productEventAggregated == null) {
                        continue;
                    }

                    String rankingKey = "products:events:view:ranking";
                    System.out.printf("Saving product events value: %s%n", record.value());
                    redisConnection.zincrby(rankingKey, productEventAggregated.counts(), productEventAggregated.productId());
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }

    private Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private ProductEventAggregated serializeMessage(String message) {
        try {
            return mapper.readValue(message, ProductEventAggregated.class);
        } catch (JsonProcessingException e) {
            System.err.println("Error to serialize message: " + message + " error: " + e.getMessage());
            return null;
        }
    }
}
