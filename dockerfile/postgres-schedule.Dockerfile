FROM postgres:15-alpine

COPY dockerfile/init-schedule.sql /docker-entrypoint-initdb.d/

EXPOSE 5432