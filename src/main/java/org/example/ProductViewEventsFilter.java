package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class ProductViewEventsFilter {

    private final String APPLICATION_ID;
    private final String CLIENT_ID;
    private final ObjectMapper mapper;
    private final String kafkaServer;
    private final String sourceTopic;
    private final String outputTopic;

    public ProductViewEventsFilter(
        String kafkaServer,
        String sourceTopic,
        String outputTopic
    ) {
        this.APPLICATION_ID = "product-view-events-filter-app";
        this.CLIENT_ID = "product-view-events-filter-client";
        this.mapper = new ObjectMapper();
        this.kafkaServer = kafkaServer;
        this.sourceTopic = sourceTopic;
        this.outputTopic = outputTopic;
    }

    public void filter() {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> source = builder.stream(sourceTopic);
            source
                .filter(this::filterViewEvents)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            KafkaStreams streams = new KafkaStreams(builder.build(), getProps());
            streams.start();
        } catch (RuntimeException e) {
            System.err.println("View messages filter error: " + e.getMessage());
        }
    }

    private boolean filterViewEvents(String key, String value) {
        try {
            JsonNode jsonNode = mapper.readTree(value);
            String eventType = jsonNode.get("eventType").asText();
            return "view".equals(eventType);
        } catch (Exception e) {
            System.err.println("Failed to parse message: " + value + " | Error: " + e.getMessage());
            return false;
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
