package org.example;

import org.example.cross.RedisConnection;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static final String PRODUCT_EVENTS_TOPIC = "product-events-topic";
    private static final String PRODUCT_VIEW_EVENTS_TOPIC = "product-view-events-topic";
    private static final String PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC = "product-view-events-aggregated-topic";

    private static final RedisConnection redisConnection = new RedisConnection("localhost", 6379, 2000);

    public static void main(String[] args) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            // Producer
            executor.submit(ProductsEventsProducer::run);

            // Filter
            ProductsEventsFilter viewEventFilter = new ProductsEventsFilter(PRODUCT_EVENTS_TOPIC, PRODUCT_VIEW_EVENTS_TOPIC);
            executor.submit(viewEventFilter::filter);

            // Aggregator
            ProductsEventsRankingAggregator viewEventAggregator = new ProductsEventsRankingAggregator(PRODUCT_VIEW_EVENTS_TOPIC, PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC);
            executor.submit(viewEventAggregator::aggregate);

            // Consumer
            Jedis connection = redisConnection.getConnection();

            ProductsEventsRankingConsumer productsEventsRankingConsumer = new ProductsEventsRankingConsumer(connection, PRODUCT_VIEW_EVENTS_AGGREGATED_TOPIC);
            executor.submit(productsEventsRankingConsumer::run);

            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("A execução foi interrompida: " + e.getMessage());
        }
    }
}