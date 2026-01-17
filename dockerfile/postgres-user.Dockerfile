# dockerfile/postgres-user.Dockerfile
FROM postgres:15-alpine

# Копируем скрипт инициализации
COPY dockerfile/init-user.sql /docker-entrypoint-initdb.d/

# Даем права на выполнение
RUN chmod 644 /docker-entrypoint-initdb.d/init-user.sql

EXPOSE 5432

# Проверяем, что файл существует и читаем
RUN ls -la /docker-entrypoint-initdb.d/