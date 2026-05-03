package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        Map<String, Double> caParVille = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("pos-events"));
            System.out.println("ChiffreAffairesParVille démarré...");

            long lastPrint = System.currentTimeMillis();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    EventPOS event = mapper.readValue(record.value(), EventPOS.class);

                    if ("VENTE".equals(event.getType())) {
                        caParVille.put(event.getVille(), caParVille.getOrDefault(event.getVille(), 0.0) + event.getMontant());
                    } else if ("RETOUR".equals(event.getType())) {
                        caParVille.put(event.getVille(), caParVille.getOrDefault(event.getVille(), 0.0) - event.getMontant());
                    }

                    System.out.printf("Lu: partition=%d, offset=%d, ville=%s, type=%s, montant=%.2f%n",
                            record.partition(), record.offset(), event.getVille(), event.getType(), event.getMontant());
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                if (System.currentTimeMillis() - lastPrint >= 5000) {
                    System.out.println("===== CA PAR VILLE =====");
                    if (caParVille.isEmpty()) {
                        System.out.println("(aucune donnée pour l'instant)");
                    } else {
                        caParVille.forEach((ville, total) ->
                                System.out.printf("%s => %.2f DT%n", ville, total));
                    }
                    lastPrint = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
