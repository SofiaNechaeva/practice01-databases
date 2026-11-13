#!/usr/bin/env bash
# Зарегистрировать источник Debezium и приёмник Elasticsearch в Kafka Connect.
set -euo pipefail
cd "$(dirname "$0")/.."

connect_url=http://localhost:8083
curl -fsS "$connect_url/connector-plugins" >/dev/null  # Проверить доступность Connect.

# REST PUT принимает внутренний объект config и обновляет существующий коннектор.
register_connector() {
  file=$1
  name=$2
  python3 -c 'import json,sys; json.dump(json.load(open(sys.argv[1]))["config"],sys.stdout)' "$file" |
    curl --fail-with-body -sS -X PUT -H 'Content-Type: application/json' \
      --data-binary @- "$connect_url/connectors/$name/config" >/dev/null
}

# Удалить прежние JSON-коннекторы: они больше не должны писать в те же приёмники.
for old in lab4-postgres-source lab4-postgres-wal2json lab4-elasticsearch-sink lab4-redis-students; do
  if curl -fsS "$connect_url/connectors/$old/status" >/dev/null 2>&1; then
    curl --fail-with-body -sS -X DELETE "$connect_url/connectors/$old" >/dev/null
  fi
done

# Применить два конфига; помощник дождётся запуска каждой задачи.
register_connector lab4/postgres-source.json lab4-postgres-avro
register_connector lab4/elasticsearch-sink.json lab4-elasticsearch-avro-sink
python3 lab4/wait-for-connector.py lab4-postgres-avro
python3 lab4/wait-for-connector.py lab4-elasticsearch-avro-sink
