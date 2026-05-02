package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import tn.utm.kafka.models.EventPOS;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ChiffreAffairesParVille {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ca-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Map<String, Double> ca = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("pos-events"));

            long lastPrint = System.currentTimeMillis();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> r : records) {
                    EventPOS e = mapper.readValue(r.value(), EventPOS.class);

                    if ("VENTE".equals(e.type)) {
                        ca.put(e.ville, ca.getOrDefault(e.ville, 0.0) + e.montant);
                    } else if ("RETOUR".equals(e.type)) {
                        ca.put(e.ville, ca.getOrDefault(e.ville, 0.0) - e.montant);
                    }

                    System.out.println("Lu: partition=" + r.partition() +
                            ", offset=" + r.offset() +
                            ", ville=" + e.ville +
                            ", type=" + e.type +
                            ", montant=" + e.montant);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                if (System.currentTimeMillis() - lastPrint >= 5000) {
                    System.out.println("===== CA PAR VILLE =====");
                    ca.forEach((ville, total) ->
                            System.out.println(ville + " => " + total));
                    lastPrint = System.currentTimeMillis();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}