up:
	docker compose -f docker/tests-docker-compose.yaml -p enco_test up -d --force-recreate --remove-orphans || true

task-3:
	docker compose -f docker/local-docker-compose.yaml -p enco_test exec postgres /bin/sh -c \
	"psql -U postgres -d enco_test < /opt/app/task_3.sql"

task-4:
	docker compose -f docker/local-docker-compose.yaml -p enco_test exec clickhouse /bin/sh -c \
	"clickhouse-client < /opt/app/task_4.sql"