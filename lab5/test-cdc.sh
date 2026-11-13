#!/usr/bin/env bash
# Проверить Create/Update/Delete студента по цепочке PostgreSQL -> Kafka -> Neo4j.
set -euo pipefail
cd "$(dirname "$0")/.."

topic=university_avro.public.students
test_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
group_id=$(docker exec practice01-postgres psql -X -At -U admin -d university_system \
  -c 'SELECT id FROM public.student_groups ORDER BY id LIMIT 1')
if [[ -z "$group_id" ]]; then
  echo 'No student_groups rows found; seed the PostgreSQL schema first.' >&2
  exit 1
fi

pg_sql() {
  docker exec practice01-postgres psql -X -v ON_ERROR_STOP=1 -U admin -d university_system "$@"
}

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

neo4j_count() {
  local query=$1
  docker exec practice01-neo4j cypher-shell -u neo4j -p neo4jsecret \
    --format plain "$query" 2>/dev/null | sed -n '2p' | tr -d '[:space:]"'
}

wait_graph() {
  local expected_name=$1 expected_count=$2
  for ((i=0; i<45; i++)); do
    local count name
    count=$(neo4j_count "MATCH (s:Student {id: '$test_id'}) RETURN count(s)")
    name=$(neo4j_count "MATCH (s:Student {id: '$test_id'})-[:MEMBER_OF]->(:StudentGroup {id: '$group_id'}) RETURN s.first_name")
    if [[ "$count" == "$expected_count" ]] && { [[ "$expected_count" == 0 ]] || [[ "$name" == "$expected_name" ]]; }; then
      echo "Neo4j: count=$count name=${name:-ABSENT}"
      return 0
    fi
    sleep 2
  done
  echo "Neo4j did not reach expected state: count=$expected_count name=$expected_name" >&2
  return 1
}

echo "Test UUID: $test_id"
offset=$(kafka_end_offset)
pg_sql -c "INSERT INTO public.students (id, group_id, first_name, last_name, email, student_card_number, enrollment_date, status) VALUES ('$test_id', '$group_id', 'CDC created', 'Neo4j', 'cdc-$test_id@example.invalid', 'CDC-${test_id:0:8}', CURRENT_DATE, 'active')"
wait_kafka "$offset"
wait_graph 'CDCcreated' 1

offset=$(kafka_end_offset)
pg_sql -c "UPDATE public.students SET first_name='CDC updated', updated_at=NOW() WHERE id='$test_id'"
wait_kafka "$offset"
wait_graph 'CDCupdated' 1

offset=$(kafka_end_offset)
pg_sql -c "DELETE FROM public.students WHERE id='$test_id'"
wait_kafka "$offset"
wait_graph '' 0

echo 'PASS: Create/Update/Delete reached Kafka and Neo4j, including MEMBER_OF.'
