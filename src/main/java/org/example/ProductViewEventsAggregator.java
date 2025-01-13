package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.example.entities.ProductEventAggregated;

import java.util.Properties;

public class ProductViewEventsAggregator {

    private final String APPLICATION_ID;
    private final String CLIENT_ID;
    private final ObjectMapper mapper;
    private final String kafkaServer;
    private final String sourceTopic;
    private final String outputTopic;

    public ProductViewEventsAggregator(
        String kafkaServer,
        String sourceTopic,
        String outputTopic
    ) {
        this.APPLICATION_ID = "product-view-events-aggregation-app";
        this.CLIENT_ID = "product-view-events-aggregation-client";
        this.mapper = new ObjectMapper();
        this.kafkaServer = kafkaServer;
        this.sourceTopic = sourceTopic;
        this.outputTopic = outputTopic;
    }

    public void aggregate() {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> source = builder.stream(sourceTopic);

            source
                .map((key, value) -> {
                    JsonNode node = this.serializeMessage(value);
                    return new KeyValue<>(node.get("productId").asText(), "view");
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("product-view-events-aggregation-store"))
                .toStream()
                .map((productId, count) ->
                    new KeyValue<>(productId, serializeOutputMessage(productId, Integer.parseInt(count + "")))
                )
                .filter((key, value) -> value != null)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            KafkaStreams streams = new KafkaStreams(builder.build(), getProps());
            streams.start();
        } catch (RuntimeException e) {
            System.err.println("Error in aggregation: " + e.getMessage());
        }
    }

    private JsonNode serializeMessage(String value) {
        try {
            return mapper.readTree(value);
        } catch (Exception e) {
            return null;
        }
    }

    private String serializeOutputMessage(String productId, int count) {
        try {
            ProductEventAggregated aggregated = new ProductEventAggregated(productId, "view", count);
            return mapper.writeValueAsString(aggregated);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties getProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, this.CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
