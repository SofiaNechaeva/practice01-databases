#!/usr/bin/env bash
# Register the shared Debezium source and the Lab4 Elasticsearch sink.
set -euo pipefail
cd "$(dirname "$0")/.."
connect_url=http://localhost:8083

bash lab4/setup-source.sh

register_connector() {
  file=$1
  name=$2
  python3 -c 'import json,sys; json.dump(json.load(open(sys.argv[1]))["config"],sys.stdout)' "$file" |
    curl --fail-with-body -sS -X PUT -H 'Content-Type: application/json'       --data-binary @- "$connect_url/connectors/$name/config" >/dev/null
}

for old in lab4-elasticsearch-sink lab4-redis-students; do
  if curl -fsS "$connect_url/connectors/$old/status" >/dev/null 2>&1; then
    curl --fail-with-body -sS -X DELETE "$connect_url/connectors/$old" >/dev/null
  fi
done

register_connector lab4/elasticsearch-sink.json lab4-elasticsearch-avro-sink
python3 lab4/wait-for-connector.py lab4-elasticsearch-avro-sink
