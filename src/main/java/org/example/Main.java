package org.example;

import org.example.cross.RedisConnection;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String PRODUCT_EVENTS_TOPIC = "product-events-topic";
    private static final String PRODUCT_VIEW_EVENTS_TOPIC = "product-view-events-topic";
    private static final String PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC = "product-view-events-aggregated-topic";
    private static final RedisConnection redisConnection = new RedisConnection("localhost", 6379, 2000);

    public static void main(String[] args) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Producer
            ProductEventsProducer producer = new ProductEventsProducer(KAFKA_SERVER, PRODUCT_EVENTS_TOPIC);
            executor.submit(producer::run);

            // Filter
            ProductViewEventsFilter viewEventsFilter = new ProductViewEventsFilter(
                KAFKA_SERVER,
                PRODUCT_EVENTS_TOPIC,
                PRODUCT_VIEW_EVENTS_TOPIC
            );
            executor.submit(viewEventsFilter::filter);


            // Aggregator
            ProductViewEventsAggregator viewEventsAggregator = new ProductViewEventsAggregator(
                KAFKA_SERVER,
                PRODUCT_VIEW_EVENTS_TOPIC,
                PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC
            );
            executor.submit(viewEventsAggregator::aggregate);

            // Consumer
            Jedis connection = redisConnection.getConnection();

            ProductViewEventsConsumer consumer = new ProductViewEventsConsumer(
                KAFKA_SERVER,
                PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC,
                connection
            );
            executor.submit(consumer::run);

            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("InterruptedException. Error: " + e.getMessage());
        }
    }
}