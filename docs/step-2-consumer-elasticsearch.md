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
  "deviseSource": "USD",
  "devise": "EUR",
  "nomDevise": "Euro",
  "zoneMonetaire": "Zone euro",
  "taux": 0.92,
  "tauxInverse": 1.086957,
  "montantPour100Usd": 92.00,
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

## Idees de dashboard

- Courbe : `Average taux` par `timestamp`, filtre `devise: EUR OR devise: XOF OR devise: XAF`.
- Bar chart : `Average montantPour100Usd` groupe par `nomDevise.keyword`.
- Metric : dernier `taux` pour `devise: XOF`.
- Metric : dernier `taux` pour `devise: XAF`.
- Data table : `devise`, `nomDevise`, `zoneMonetaire`, `taux`, `tauxInverse`, `montantPour100Usd`.

Les champs utiles pour le dashboard sont :

- `deviseSource` : devise de base, ici `USD`.
- `devise` : code de la devise cible, par exemple `EUR`, `XOF`, `XAF`.
- `nomDevise` : libelle lisible, par exemple `Franc CFA BCEAO`.
- `zoneMonetaire` : zone geographique de la devise.
- `taux` : montant de devise cible pour 1 USD.
- `tauxInverse` : valeur inverse du taux.
- `montantPour100Usd` : conversion directe de 100 USD.
