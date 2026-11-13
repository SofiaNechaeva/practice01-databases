#!/usr/bin/env bash
# Запустить Kafka и проверить плагины, нужные лабораторной.
set -euo pipefail
cd "$(dirname "$0")/.."

# Compose собирает образ Kafka Connect из Dockerfile.connect.
docker compose -f lab4/docker-compose.kafka.yml up -d --build

# Дождаться Schema Registry: AvroConverter обращается к нему при публикации события.
curl -fsS --retry 45 --retry-delay 2 --retry-connrefused --retry-all-errors \
  http://localhost:18081/subjects >/dev/null

# REST API Connect появляется позже контейнера; curl повторяет попытки подключения.
curl -fsS --retry 45 --retry-delay 2 --retry-connrefused --retry-all-errors \
  http://localhost:8083/connector-plugins |
  python3 -c '
import json, sys
plugins = {item["class"] for item in json.load(sys.stdin)}
required = {
    "io.debezium.connector.postgresql.PostgresConnector",
    "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "org.neo4j.connectors.kafka.sink.Neo4jConnector",
    "com.mongodb.kafka.connect.MongoSinkConnector",
}
missing = required - plugins
if missing:
    raise SystemExit(f"Missing Kafka Connect plugins: {sorted(missing)}")
print("Debezium, Elasticsearch, Neo4j and MongoDB Sink plugins are available")
'

echo 'Shared Kafka, Schema Registry, Kafka Connect and Kafka UI for Lab4/Lab5 are ready.'
