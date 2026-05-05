# Kibana - forex-data

L'application publie le JSON complet de l'API `https://api.exchangerate-api.com/v4/latest/USD` dans Kafka, puis indexe chaque reponse dans Elasticsearch.

Index Elasticsearch :

```text
forex-data
```

Topic Kafka :

```text
exchange-rates
```

Data view Kibana 8.12.0 :

```text
forex-data*
```

Champ temps a choisir :

```text
@timestamp
```

Champs utiles dans Discover ou Lens :

```text
base
rates.EUR
rates.XOF
rates.XAF
rates.CAD
rates.GBP
rates.JPY
timestamp
```

Commande pour ajouter une nouvelle reponse API dans Kafka et Elasticsearch :

```bash
curl -X POST http://localhost:8081/exchange-rates/refresh
```

Si les champs `rates.*` n'apparaissent pas dans Kibana, rafraichir la liste des champs de la data view apres avoir envoye au moins une nouvelle donnee.
