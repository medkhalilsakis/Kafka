package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import tn.utm.kafka.models.EventPOS;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SimulateurCaisse {

    private static final List<String> VILLES = List.of("Tunis", "Sousse", "Sfax", "Bizerte", "Gabès");
    private static final List<String> PRODUITS = List.of("pain", "lait", "fromage", "eau", "riz", "huile", "yaourt", "pâtes");

    public static void main(String[] args) throws Exception {
        String caisseId = args.length > 0 ? args[0] : "CAISSE-" + ThreadLocalRandom.current().nextInt(1, 100);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Simulateur démarré pour " + caisseId + " ... Ctrl+C pour arrêter.");
            while (true) {
                EventPOS event = new EventPOS();
                event.setIdCaisse(caisseId);
                event.setVille(VILLES.get(random.nextInt(VILLES.size())));
                event.setTimestamp(Instant.now().toString());

                int choix = random.nextInt(100);
                if (choix < 70) {
                    event.setType("VENTE");
                } else if (choix < 80) {
                    event.setType("RETOUR");
                } else {
                    event.setType("OUVERTURE");
                }

                if ("VENTE".equals(event.getType()) || "RETOUR".equals(event.getType())) {
                    double montant = 5 + (random.nextDouble() * 495);
                    event.setMontant(Math.round(montant * 100.0) / 100.0);
                    event.setProduits(randomProducts(random));
                } else {
                    event.setMontant(0.0);
                    event.setProduits(new String[0]);
                }

                String json = mapper.writeValueAsString(event);
                ProducerRecord<String, String> record = new ProducerRecord<>("pos-events", event.getVille(), json);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Erreur envoi: " + exception.getMessage());
                        } else {
                            System.out.printf("Envoyé: ville=%s, type=%s, partition=%d, offset=%d%n",
                                    event.getVille(), event.getType(), metadata.partition(), metadata.offset());
                        }
                    }
                });

                Thread.sleep(100 + random.nextInt(401));
            }
        }
    }

    private static String[] randomProducts(Random random) {
        int count = 1 + random.nextInt(4);
        String[] items = new String[count];
        for (int i = 0; i < count; i++) {
            items[i] = PRODUITS.get(random.nextInt(PRODUITS.size()));
        }
        return items;
    }
}
