# TP Kafka — Mini-projet : pipeline de logs en temps réel

## 2. Prérequis
- Java 17+
- Maven
- Kafka démarré en KRaft sur `localhost:9092`

## 3. Création des topics
```powershell
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic pos-events --partitions 4 --replication-factor 1
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic alertes-retours --partitions 1 --replication-factor 1
```

## 4. Compilation
```powershell
mvn clean package
```

## 5. Lancer les programmes

### Simulateur de caisse
```powershell
mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimulateurCaisse"
```

Optionnellement, préciser l'identifiant de caisse :
```powershell
mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimulateurCaisse" -Dexec.args="CAISSE-TUNIS-03"
```

### Chiffre d'affaires par ville
```powershell
mvn exec:java -Dexec.mainClass="tn.utm.kafka.ChiffreAffairesParVille"
```

### Détecteur d'anomalies
```powershell
mvn exec:java -Dexec.mainClass="tn.utm.kafka.DetecteurAnomalies"
```

## 6. Groupes Kafka
Afficher le lag du groupe CA :
```powershell
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group ca-1
```

Afficher le lag du groupe alerte :
```powershell
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group alerte-1
```

## 7. Remarques
- La clé Kafka est la `ville` pour conserver l'ordre des événements d'une même ville.
- `ChiffreAffairesParVille` utilise un commit manuel des offsets.
- `DetecteurAnomalies` envoie les retours supérieurs à 200 DT vers `alertes-retours`.
- Avec plusieurs consommateurs dans le groupe `ca-1`, Kafka répartit les partitions automatiquement.

## 8. Structure
- `tn.utm.kafka.models.EventPOS`
- `tn.utm.kafka.SimulateurCaisse`
- `tn.utm.kafka.ChiffreAffairesParVille`
- `tn.utm.kafka.DetecteurAnomalies`
