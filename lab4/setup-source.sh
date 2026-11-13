#!/usr/bin/env bash
# Register the shared PostgreSQL Debezium source used by Lab4 and Lab5.
set -euo pipefail
cd "$(dirname "$0")/.."
connect_url=http://localhost:8083
curl -fsS "$connect_url/connector-plugins" >/dev/null

for old in lab4-postgres-source lab4-postgres-wal2json lab5-postgres-wal2json; do
  if curl -fsS "$connect_url/connectors/$old/status" >/dev/null 2>&1; then
    curl --fail-with-body -sS -X DELETE "$connect_url/connectors/$old" >/dev/null
  fi
done

python3 -c 'import json,sys; json.dump(json.load(open(sys.argv[1]))["config"],sys.stdout)' lab4/postgres-source.json |
  curl --fail-with-body -sS -X PUT -H 'Content-Type: application/json'     --data-binary @- "$connect_url/connectors/lab4-postgres-avro/config" >/dev/null
python3 lab4/wait-for-connector.py lab4-postgres-avro
