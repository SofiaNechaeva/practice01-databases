## Создание студента в postgres (Create)
export TEST_ID=$(uuidgen)
export GROUP_ID=$(docker exec practice01-postgres psql -X -At -U admin -d university_system \
  -c 'SELECT id FROM public.student_groups ORDER BY id LIMIT 1')

docker exec practice01-postgres psql -U admin -d university_system -c \
"INSERT INTO public.students
 (id, group_id, first_name, last_name, email, student_card_number, enrollment_date, status)
 VALUES
 ('$TEST_ID', '$GROUP_ID', 'Manual created', 'CDC',
  'manual-$TEST_ID@example.invalid', 'MANUAL-${TEST_ID:0:8}', CURRENT_DATE, 'active')"

## Проверить публикацию в Kafka:

Avro-сообщения не выводятся штатным console consumer как JSON. До и после операции сравните end offset топика:

~~~bash
docker compose -f lab4/docker-compose.kafka.yml exec -T broker \
  kafka-get-offsets --bootstrap-server broker:29092 \
  --topic university_avro.public.students
~~~

## Проверить Neo4j:
docker exec practice01-neo4j cypher-shell -u neo4j -p neo4jsecret \
  "MATCH (s:Student {id: '$TEST_ID'})-[:MEMBER_OF]->(g:StudentGroup)
   RETURN s.first_name, s.last_name, g.name"

## Проверить Mongodb:
docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('university_system').students_cdc.findOne({_id:{id:'$TEST_ID'}})"

## Update:
docker exec practice01-postgres psql -U admin -d university_system -c \
"UPDATE public.students
 SET first_name='Manual updated', updated_at=NOW()
 WHERE id='$TEST_ID'"

## Delete:
docker exec practice01-postgres psql -U admin -d university_system -c \
"DELETE FROM public.students WHERE id='$TEST_ID'"

## Просмотр иерархии mongodb

docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('university_system').university_hierarchy_cdc.findOne()"

Если иерархия не отображается полностью:

docker exec practice01-postgres psql -U admin -d university_system -c \
"UPDATE public.universities SET updated_at = NOW()"
docker exec practice01-postgres psql -U admin -d university_system -c \
"UPDATE public.institutes SET updated_at = NOW()"
docker exec practice01-postgres psql -U admin -d university_system -c \
"UPDATE public.departments SET updated_at = NOW()"

## Автоматическая проверка MongoDB-иерархии

~~~bash
bash lab5/test-mongodb-hierarchy-cdc.sh
~~~

Скрипт выводит UUID временных университета, института и кафедры. Пока тест выполняется, их вложенный документ можно посмотреть так:

~~~bash
export HIERARCHY_UNIVERSITY_ID='<UUID из Test university>'
export HIERARCHY_INSTITUTE_ID='<UUID из Test institute>'
export HIERARCHY_DEPARTMENT_ID='<UUID из Test department>'

docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret   --authenticationDatabase admin   --eval "db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$HIERARCHY_UNIVERSITY_ID'})"
~~~

Проверка кафедры после Create или Update:

~~~bash
docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret   --authenticationDatabase admin   --eval "const d=db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$HIERARCHY_UNIVERSITY_ID'}); printjson(d.institutes['$HIERARCHY_INSTITUTE_ID'].departments['$HIERARCHY_DEPARTMENT_ID'])"
~~~

После Delete кафедры или института соответствующее поле отсутствует; после Delete университета запрос вернёт null:

~~~bash
docker exec practice01-mongodb mongosh --quiet -u appuser -p appsecret   --authenticationDatabase admin   --eval "db.getSiblingDB('university_system').university_hierarchy_cdc.findOne({_id:'$HIERARCHY_UNIVERSITY_ID'})"
~~~
