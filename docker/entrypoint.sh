#!/bin/bash
set -e

# 1. Запускаем Greenplum (стандартная команда из образа)
# (Мы запускаем его в фоне, чтобы успеть накатить скрипты)
/docker-entrypoint.sh postgres &
GP_PID=$!

# 2. Ждем, пока база проснется
echo "Waiting for Greenplum to start..."
until psql -U gpadmin -d postgres -c '\q' > /dev/null 2>&1; do
  sleep 2
done
echo "Greenplum started!"

# 3. Выполняем твои скрипты
echo "Running initialization scripts..."
psql -U gpadmin -d postgres -c "CREATE DATABASE demo_db;" || true
psql -U gpadmin -d demo_db -f /code/docker/init.sql
psql -U gpadmin -d demo_db -f /code/src/install.sql

echo "Initialization complete!"

# 4. Ждем завершения процесса GP (чтобы контейнер не закрылся)
wait "$GP_PID"
