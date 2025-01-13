package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.entities.ProductEvent;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.example.EventsMock.getRandomProductEvent;

public class ProductEventsProducer {

    private final ObjectMapper mapper;
    private final String kafkaServer;
    private final String topic;

    public ProductEventsProducer(
        String kafkaServer,
        String topic
    ) {
        this.mapper = new ObjectMapper();
        this.kafkaServer = kafkaServer;
        this.topic = topic;
    }

    public void run() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps())) {
            while (true) {
                String uniqueId = UUID.randomUUID().toString();
                ProductEvent productEvent = getRandomProductEvent();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, uniqueId, serialize(productEvent));
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf(
                    "Message sent: [topic=%s, partition=%d, offset=%d, id=%s]%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), uniqueId
                );
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private String serialize(ProductEvent productEvent) {
        try {
            return this.mapper.writeValueAsString(productEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
