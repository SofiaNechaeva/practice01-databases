#!/usr/bin/env bash
# Checks Create/Update/Delete: PostgreSQL -> Avro Kafka -> MongoDB hierarchy.
set -euo pipefail
cd "$(dirname "$0")/.."

university_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
institute_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
department_id=$(python3 -c 'import uuid; print(uuid.uuid4())')

pg_sql() {
  docker exec practice01-postgres psql -X -v ON_ERROR_STOP=1 -U admin -d university_system "$@"
}
mongo_eval() {
  docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret --authenticationDatabase admin --eval "$1" | tr -d '[:space:]'
}
wait_mongo() {
  local expected=$1 expression=$2 description=$3
  for ((i=0; i<45; i++)); do
    if [[ "$(mongo_eval "$expression")" == "$expected" ]]; then
      echo "MongoDB: $description"
      return 0
    fi
    sleep 2
  done
  echo "MongoDB did not reach expected state: $description" >&2
  return 1
}

university_exists="db.getSiblingDB('university_system').university_hierarchy_cdc.countDocuments({_id:'$university_id'})"
institute_exists="const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$university_id'}); print(d && d.institutes && d.institutes['$institute_id'] ? 1 : 0)"
department_created="const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$university_id'}); print(d && d.institutes && d.institutes['$institute_id'] && d.institutes['$institute_id'].departments && d.institutes['$institute_id'].departments['$department_id'] && d.institutes['$institute_id'].departments['$department_id'].name === 'Hierarchy created' ? 1 : 0)"
department_updated="const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$university_id'}); print(d && d.institutes && d.institutes['$institute_id'] && d.institutes['$institute_id'].departments && d.institutes['$institute_id'].departments['$department_id'] && d.institutes['$institute_id'].departments['$department_id'].name === 'Hierarchy updated' ? 1 : 0)"
department_deleted="const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$university_id'}); print(d && d.institutes && d.institutes['$institute_id'] && (!d.institutes['$institute_id'].departments || !d.institutes['$institute_id'].departments['$department_id']) ? 1 : 0)"
institute_deleted="const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$university_id'}); print(d && (!d.institutes || !d.institutes['$institute_id']) ? 1 : 0)"

echo "Test university: $university_id"
echo "Test institute: $institute_id"
echo "Test department: $department_id"

pg_sql -c "INSERT INTO public.universities (id, name, short_name) VALUES ('$university_id', 'Hierarchy test university', 'HTU')"
wait_mongo 1 "$university_exists" 'university root was created'
pg_sql -c "INSERT INTO public.institutes (id, university_id, name, short_name) VALUES ('$institute_id', '$university_id', 'Hierarchy test institute', 'HTI')"
wait_mongo 1 "$institute_exists" 'institute was embedded'
pg_sql -c "INSERT INTO public.departments (id, institute_id, name, short_name) VALUES ('$department_id', '$institute_id', 'Hierarchy created', 'HCD')"
wait_mongo 1 "$department_created" 'department was embedded (Create)'

pg_sql -c "UPDATE public.departments SET name='Hierarchy updated', updated_at=NOW() WHERE id='$department_id'"
wait_mongo 1 "$department_updated" 'department was updated'

pg_sql -c "DELETE FROM public.departments WHERE id='$department_id'"
wait_mongo 1 "$department_deleted" 'department was removed (Delete)'
pg_sql -c "DELETE FROM public.institutes WHERE id='$institute_id'"
wait_mongo 1 "$institute_deleted" 'institute was removed (Delete)'
pg_sql -c "DELETE FROM public.universities WHERE id='$university_id'"
wait_mongo 0 "$university_exists" 'university document was removed (Delete)'
echo 'PASS: Create/Update/Delete reached MongoDB hierarchy.'
