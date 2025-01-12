package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.entities.ProductEventAggregated;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ProductsEventsRankingConsumer {

    private final Jedis redisConnection;
    private final String sourceTopic;
    private final ObjectMapper mapper;

    public ProductsEventsRankingConsumer(Jedis redisConnection, String sourceTopic) {
        this.redisConnection = redisConnection;
        this.sourceTopic = sourceTopic;
        this.mapper = new ObjectMapper();
    }

    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProducerProps());
        consumer.subscribe(Collections.singletonList(sourceTopic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    ProductEventAggregated productEventAggregated = serializeMessage(record.value());

                    if (productEventAggregated == null) {
                        continue;
                    }

                    String rankingKey = getRedisEventKey(productEventAggregated.eventType());
                    System.out.printf("Saving product events value: %s%n", record.value());
                    redisConnection.zincrby(rankingKey, productEventAggregated.counts(), productEventAggregated.productId());
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "product-events-aggregation-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

    private String getRedisEventKey(String eventType) {
        return "products:events:" + eventType + ":ranking";
    }
}
