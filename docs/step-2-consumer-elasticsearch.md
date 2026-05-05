# Step 2 - Consumer Kafka et Elasticsearch

## Role de cette partie

Cette partie correspond au travail cote consommateur :

1. Lire les messages Kafka du topic `exchange-rates`.
2. Parser le message JSON envoye par le producteur.
3. Indexer chaque message dans Elasticsearch.
4. Valider la visualisation dans Kibana.

## Configuration

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=exchange-rate-indexer
exchange.kafka.topic=exchange-rates
exchange.elasticsearch.index=exchange-rates
spring.elasticsearch.uris=http://localhost:9200
```

## Topic Kafka

Le topic attendu est :

```bash
kafka-topics.sh --create --topic exchange-rates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Message attendu

Le consumer accepte un message JSON envoye par le producer, par exemple :

```json
{
  "devise": "EUR",
  "taux": 0.92,
  "timestamp": "2026-05-05T10:00:00Z"
}
```

Chaque message est indexe dans Elasticsearch dans l'index `exchange-rates`.

## Dashboard Kibana

1. Ouvrir Kibana sur `http://localhost:5601`.
2. Aller dans `Stack Management`.
3. Aller dans `Data Views`.
4. Creer un data view `exchange-rates`.
5. Choisir `indexedAt` comme champ temporel si Kibana le propose.
6. Creer une visualisation avec le taux par devise dans le temps.

La validation est faite quand les nouveaux messages Kafka apparaissent dans Kibana apres indexation Elasticsearch.
