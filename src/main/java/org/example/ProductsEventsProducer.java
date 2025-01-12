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

public class ProductsEventsProducer {

    private static final String PRODUCT_EVENTS_TOPIC = "product-events-topic";
    private static final ObjectMapper mapper = new ObjectMapper();

    private ProductsEventsProducer() {}

    public static void run() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProps());

        try {
            while (true) {
                String uniqueId = UUID.randomUUID().toString();
                ProductEvent productEvent = getRandomProductEvent();
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    PRODUCT_EVENTS_TOPIC,
                    uniqueId,
                    serialize(productEvent)
                );
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf(
                    "Message sent: [tópico=%s, partição=%d, offset=%d, id=%s]%n",
                    metadata.topic(), metadata.partition(), metadata.offset(), uniqueId
                );

                Thread.sleep(500);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static String serialize(ProductEvent productEvent) {
        try {
            return mapper.writeValueAsString(productEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
