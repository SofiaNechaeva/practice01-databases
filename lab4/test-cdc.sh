#!/usr/bin/env bash
# Проверить схему в Registry и Create/Update/Delete через PostgreSQL, Kafka и Elasticsearch.
set -euo pipefail
cd "$(dirname "$0")/.."  # Использовать пути Compose от корня проекта.

# Новый UUID позволяет не трогать данные генератора.
topic=university_avro.public.universities
test_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
es_id=$(printf '{"id":"%s"}' "$test_id")
pg_sql() {
  docker exec practice01-postgres psql -X -v ON_ERROR_STOP=1 \
    -U admin -d university_system "$@"
}

# Elasticsearch использует JSON-ключ Kafka как _id; искать точное значение.
es_document() {
  python3 -c 'import json,sys; print(json.dumps({"query":{"ids":{"values":[sys.argv[1]]}}}))' "$es_id" |
    curl -fsS -H 'Content-Type: application/json' --data-binary @- \
      "http://localhost:9200/$topic/_search" 2>/dev/null
}

# Refresh делает свежую запись видимой в поиске; затем сравнить документ.
wait_elasticsearch() {
  local expected_name=$1
  for ((i=0; i<60; i++)); do
    curl -fsS -X POST "http://localhost:9200/$topic/_refresh" >/dev/null 2>&1 || true
    if es_document | python3 -c '
import json,sys
expected=sys.argv[1]
hits=json.load(sys.stdin)["hits"]["hits"]
if expected == "ABSENT": sys.exit(0 if not hits else 1)
if len(hits)!=1: sys.exit(1)
source=hits[0]["_source"]
row=source.get("after") if isinstance(source.get("after"),dict) else source
sys.exit(0 if row.get("name")==expected else 1)
' "$expected_name" 2>/dev/null; then
      echo "Elasticsearch: ${expected_name}"
      return 0
    fi
    sleep 2
  done
  echo "Elasticsearch did not reach expected state: $expected_name, id=$test_id" >&2
  return 1
}

echo "Test UUID: $test_id"
echo "Elasticsearch _id: $es_id"
# Схема Avro регистрируется при первой публикации; топик создаёт Debezium.
curl -fsS http://localhost:8083/connectors/lab4-postgres-avro/status |
  python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["tasks"] and all(t["state"]=="RUNNING" for t in d["tasks"])'

# Создать университет и дождаться его появления в Kafka и Elasticsearch.
pg_sql -c "INSERT INTO public.universities (id,name,short_name) VALUES ('$test_id','CDC test: created','CDC-TEST')"
wait_elasticsearch 'CDC test: created'
docker compose -f lab4/docker-compose.kafka.yml exec -T broker \
  kafka-topics --bootstrap-server broker:29092 --describe --topic "$topic" >/dev/null

# Обновить эту же строку; в Elasticsearch изменится тот же документ.
pg_sql -c "UPDATE public.universities SET name='CDC test: updated', updated_at=now() WHERE id='$test_id'"
wait_elasticsearch 'CDC test: updated'

# Удалить строку; в Kafka нужен op=d, а в Elasticsearch документа уже нет.
pg_sql -c "DELETE FROM public.universities WHERE id='$test_id'"
wait_elasticsearch ABSENT

curl -fsS "http://localhost:18081/subjects/${topic}-value/versions/latest" |
  python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["id"] > 0; print("Schema Registry: subject registered, id", d["id"])'
echo 'PASS: Avro schema registered; Create/Update/Delete reached Elasticsearch through Kafka.'
