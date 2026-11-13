# Лабораторная №4: PostgreSQL → Kafka (Avro + Schema Registry) → Elasticsearch / Redis

## Экспериментальная ветка с Avro

Значения событий Lab4 передаются через `AvroConverter`: он регистрирует схемы в Schema Registry. Ключи оставлены в прежнем JSON-формате для совместимости с идентификаторами документов и Redis. Новые топики имеют префикс `university_avro.public.*`; прежние JSON-топики `university.public.*` не читаются новыми sink-коннекторами. Для нового initial snapshot используются отдельные имя source-коннектора и слот `lab4_avro`. Конфигурация Lab5 не меняется: у неё собственные Kafka и Connect.

Перед запуском перенесите обновлённую папку `lab4` в WSL. `setup-connectors.sh` удалит старые JSON-коннекторы Lab4 из Connect и зарегистрирует новые. Старые сообщения Kafka остаются в прежних топиках. PostgreSQL volume удалять не требуется.

Порядок первой проверки: `bash lab4/setup-all.sh`, затем `bash lab4/setup-redis.sh`, `bash lab4/test-cdc.sh` и `bash lab4/test-redis.sh`. Если старый слот `lab4_wal2json` больше не нужен, после остановки старого source-коннектора проверьте его состояние:

```bash
docker exec practice01-postgres psql -U admin -d university_system -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name IN ('lab4_wal2json', 'lab4_avro')"
```

Неиспользуемый неактивный слот удерживает WAL. Его можно удалить отдельно командой `SELECT pg_drop_replication_slot('lab4_wal2json');`; перед этим убедитесь, что старый коннектор больше не нужен для возврата к JSON-ветке.

Schema Registry доступен с WSL по `http://localhost:18081`, внутри Compose-сети по `http://schema-registry:8081`. Порт 8081 на компьютере уже занят Redis Commander.

Команды выполняются в Ubuntu WSL из корня проекта `~/practice01-databases`. Перед запуском убедитесь, что в WSL находится **вся обновлённая папка проекта**, включая `postgres/postgresql.conf`. Файл конфигурации хранится в проекте и монтируется в PostgreSQL через `all-in-one/docker-compose.yaml`; копировать его из работающего контейнера больше не требуется. Если файлы копировали через Windows и Bash сообщает `invalid option name: pipefail`, переведите окончания строк скриптов в формат LF: `sed -i 's/\r$//' lab4/*.sh`. В репозитории правило `*.sh text eol=lf` записано в `.gitattributes` для следующих Git checkout.

Основные файлы:

| Файл | Назначение |
| --- | --- |
| `setup-wal2json.sh` | Собирает PostgreSQL с `wal2json`, проверяет `postgresql.conf` и публикацию `pub`. |
| `setup-kafka.sh` | Поднимает Kafka и Connect, проверяет наличие плагинов Debezium и Elasticsearch. |
| `setup-connectors.sh` | Применяет Avro-конфигурации `postgres-source.json` и `elasticsearch-sink.json`. |
| `setup-all.sh` | Запускает предыдущие этапы и Elasticsearch в нужном порядке. |
| `setup-redis.sh` | Запускает Redis, устанавливает Lenses Sink, применяет `redis-sink.json` и поднимает обработчик удалений. |
| `test-cdc.sh`, `test-redis.sh` | Проверяют полный цикл Create/Update/Delete на новых тестовых строках. |
| `wait-for-connector.py` | Ждёт состояния `RUNNING` у коннектора и его задачи; при ошибке печатает `trace`. |

```bash
cd ~/practice01-databases
bash lab4/setup-all.sh
```

Можно запускать этапы по отдельности:

```bash
bash lab4/setup-wal2json.sh    # PostgreSQL 16, wal2json, конфиг и публикация
docker compose -f all-in-one/docker-compose.yaml up --no-deps --wait elasticsearch
bash lab4/setup-kafka.sh       # Kafka, Schema Registry, Connect и плагины
bash lab4/setup-connectors.sh  # Debezium source и Elasticsearch sink
```

`setup-all.sh` запускает эти этапы в нужном порядке и ждёт готовности Elasticsearch. Скрипты повторяемы: они не удаляют Docker volumes и данные. При повторном запуске используется существующий replication slot и offsets Kafka Connect; initial snapshot не запускается заново автоматически.

Настройки источника: `lab4/postgres-source.json`. Настройки приёмника: `lab4/elasticsearch-sink.json`. Изменения в этих файлах применяются повторным запуском скрипта. После первого запуска проверьте наличие subject `university_avro.public.universities-value` в Schema Registry.

## Проверка

```bash
docker exec practice01-postgres psql -U admin -d university_system -c 'SHOW config_file'
docker exec practice01-postgres psql -U admin -d university_system -c "SELECT name, setting, sourcefile FROM pg_settings WHERE name IN ('wal_level', 'max_wal_senders', 'max_replication_slots', 'output_plugin_libraries') ORDER BY name"
docker exec practice01-postgres psql -U admin -d university_system -c 'SELECT pubname, puballtables FROM pg_publication'
curl -fsS http://localhost:8083/connector-plugins | python3 -m json.tool
curl -fsS http://localhost:8083/connectors/lab4-postgres-avro/status | python3 -m json.tool
curl -fsS http://localhost:8083/connectors/lab4-elasticsearch-avro-sink/status | python3 -m json.tool
curl -fsS http://localhost:18081/subjects | python3 -m json.tool
curl -fsS 'http://localhost:9200/_cat/indices?v'
```

Ожидается `config_file=/etc/postgresql/postgresql.conf`, `wal_level=logical`, библиотека `wal2json`, публикация `pub` с `puballtables=t` и оба коннектора вместе с задачами в состоянии `RUNNING`. Kafka UI: <http://localhost:8084>. Топики имеют вид `university_avro.public.<таблица>`.

Для проверки Create/Update/Delete создайте отдельную тестовую строку в PostgreSQL, проверьте события `c/u/d` в Kafka UI и изменения документа в Elasticsearch. В текущей проверенной конфигурации Elasticsearch использует полный JSON ключ Kafka как `_id`, например `{"id":"UUID"}`; запрос по одному UUID в `_doc/<UUID>` не найдёт документ. Поиск по полю `after.id.keyword` или просмотр `_search` покажет его.

Автоматическая проверка регистрации Avro-схемы и полного цикла Create/Update/Delete:

```bash
bash lab4/test-cdc.sh
```

Скрипт печатает UUID тестовой строки, проверяет топик `university_avro.public.universities`, регистрацию его схемы и документ Elasticsearch после каждой операции. После успешного теста строка удалена из PostgreSQL и Elasticsearch. События `c/u/d` в Avro можно посмотреть в Kafka UI; обычный `kafka-console-consumer` покажет бинарные данные без декодирования.

## Redis

Для Redis есть отдельный этап:

```bash
bash lab4/setup-redis.sh
bash lab4/test-redis.sh
```

`setup-redis.sh` запускает уже описанный в `all-in-one/docker-compose.yaml` Redis, скачивает Redis Sink из релиза 10.0.1 [Stream Reactor](https://github.com/lensesio/stream-reactor/releases), пересобирает Kafka Connect и регистрирует sink только для `university_avro.public.students`. Lenses Sink записывает и обновляет Redis cache с ключом `id`. Отдельный `redis-delete-worker` декодирует Avro через Schema Registry, читает CDC-события `op=d` и удаляет ключ; [документация Lenses](https://docs.lenses.io/latest/connectors/kafka-connectors/sinks/redis) не обещает удаление по tombstone. Тест создаёт отдельного студента и проверяет Redis после каждой операции. Если GitHub изменит имя архива релиза, установщик покажет список доступных файлов.

Прежняя JSON-версия проходила оба теста в Ubuntu WSL. Ветка с Avro требует повторного запуска `test-cdc.sh` и `test-redis.sh` после развёртывания. Для отчёта сохраните список subject Schema Registry, статусы коннекторов и лог `redis-delete-worker` с UUID теста. Генератор университетских данных запускается отдельно, только если схема отсутствует.
