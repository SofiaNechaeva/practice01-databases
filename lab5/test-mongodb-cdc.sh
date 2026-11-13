#!/usr/bin/env bash
# Проверяет Create/Update/Delete: PostgreSQL -> Kafka -> MongoDB PostgresHandler.
set -euo pipefail
cd "$(dirname "$0")/.."

topic=university_avro.public.students
test_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
group_id=$(docker exec practice01-postgres psql -X -At -U admin -d university_system \
  -c 'SELECT id FROM public.student_groups ORDER BY id LIMIT 1')
[[ -n "$group_id" ]] || { echo 'No student_groups rows found.' >&2; exit 1; }

pg_sql() { docker exec practice01-postgres psql -X -v ON_ERROR_STOP=1 -U admin -d university_system "$@"; }
kafka_end_offset() {
  docker compose -f lab4/docker-compose.kafka.yml exec -T broker     kafka-get-offsets --bootstrap-server broker:29092 --topic "$topic" |
    awk -F: '$1 == topic && $2 == 0 {print $3}' topic="$topic"
}
wait_kafka() {
  local previous=$1
  for ((i=0; i<30; i++)); do
    local current
    current=$(kafka_end_offset)
    if [[ "$current" =~ ^[0-9]+$ ]] && (( current > previous )); then
      echo "Kafka: offset $previous -> $current"
      return 0
    fi
    sleep 2
  done
  echo "Kafka did not receive a new Avro record for $topic" >&2
  return 1
}

mongo_count() {
  local name=$1
  docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret --authenticationDatabase admin \
    --eval "db.getSiblingDB('university_system').students_cdc.countDocuments({_id:{id:'$test_id'}${name:+,first_name:'$name'}})" | tr -d '[:space:]'
}
wait_mongo() {
  local name=$1 expected=$2
  for ((i=0; i<30; i++)); do
    [[ "$(mongo_count "$name")" == "$expected" ]] && { echo "MongoDB: count=$expected"; return; }
    sleep 2
  done
  echo "MongoDB did not reach expected state." >&2; return 1
}

echo "Test UUID: $test_id"
offset=$(kafka_end_offset)
pg_sql -c "INSERT INTO public.students (id, group_id, first_name, last_name, email, student_card_number, enrollment_date, status) VALUES ('$test_id', '$group_id', 'Mongo created', 'CDC', 'mongo-$test_id@example.invalid', 'MONGO-${test_id:0:8}', CURRENT_DATE, 'active')"
wait_kafka "$offset"; wait_mongo 'Mongo created' 1
offset=$(kafka_end_offset)
pg_sql -c "UPDATE public.students SET first_name='Mongo updated', updated_at=NOW() WHERE id='$test_id'"
wait_kafka "$offset"; wait_mongo 'Mongo updated' 1
offset=$(kafka_end_offset)
pg_sql -c "DELETE FROM public.students WHERE id='$test_id'"
wait_kafka "$offset"; wait_mongo '' 0
echo 'PASS: Create/Update/Delete reached MongoDB.'
