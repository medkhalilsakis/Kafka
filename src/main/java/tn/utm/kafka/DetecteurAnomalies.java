package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tn.utm.kafka.models.EventPOS;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DetecteurAnomalies {
    public static void main(String[] args) {
        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alerte-1");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Properties pProps = new Properties();
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pProps.put(ProducerConfig.ACKS_CONFIG, "all");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(pProps)) {

            consumer.subscribe(Collections.singletonList("pos-events"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> r : records) {
                    EventPOS e = mapper.readValue(r.value(), EventPOS.class);

                    if ("RETOUR".equals(e.type) && e.montant > 200) {
                        String alerte = "ALERTE RETOUR > 200 DT : " + r.value();
                        producer.send(new ProducerRecord<>("alertes-retours", e.ville, alerte));
                        System.out.println(alerte);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}