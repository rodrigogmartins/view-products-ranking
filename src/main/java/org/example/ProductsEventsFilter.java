package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.entities.ProductEventAggregated;

import java.util.Objects;
import java.util.Properties;

public class ProductsEventsFilter {

    private final String sourceTopic;
    private final String outputTopic;
    private final String APPLICATION_ID;
    private final String CLIENT_ID;
    private final ObjectMapper mapper;

    public ProductsEventsFilter(
        String sourceTopic,
        String outputTopic
    ) {
        this.sourceTopic = sourceTopic;
        this.outputTopic = outputTopic;
        this.APPLICATION_ID = "product-view-events-filter-app";
        this.CLIENT_ID = "product-view-events-filter-client";
        this.mapper = new ObjectMapper();
    }

    public void filter() {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> source = builder.stream(sourceTopic);

            source
                .mapValues(this::serializeMessage)
                .filter((key, value) ->
                    value != null && Objects.equals(value.get("eventType").asText(), "view")
                )
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
            return null;
        }
    }

    private Properties getProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }
}
