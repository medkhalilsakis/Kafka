package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import tn.utm.kafka.models.EventPOS;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class SimulateurCaisse {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        String[] villes = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabès"};
        String[] produitsPool = {"pain", "lait", "fromage", "eau", "riz", "huile"};
        Random random = new Random();
        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                EventPOS e = new EventPOS();
                e.idCaisse = "CAISSE-" + (1 + random.nextInt(3));
                e.ville = villes[random.nextInt(villes.length)];
                e.timestamp = Instant.now().toString();

                int r = random.nextInt(100);
                if (r < 70) e.type = "VENTE";
                else if (r < 80) e.type = "RETOUR";
                else e.type = "OUVERTURE";

                if ("VENTE".equals(e.type) || "RETOUR".equals(e.type)) {
                    e.montant = 5 + random.nextDouble() * 495;
                    e.produits = new String[] {
                            produitsPool[random.nextInt(produitsPool.length)]
                    };
                } else {
                    e.montant = 0;
                    e.produits = new String[0];
                }

                String json = mapper.writeValueAsString(e);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("pos-events", e.ville, json);

                producer.send(record, (meta, ex) -> {
                    if (ex != null) {
                        System.out.println("Erreur envoi: " + ex.getMessage());
                    } else {
                        System.out.println("Envoyé -> ville=" + e.ville +
                                ", partition=" + meta.partition() +
                                ", offset=" + meta.offset());
                    }
                });

                Thread.sleep(100 + random.nextInt(401));
            }
        }
    }
}