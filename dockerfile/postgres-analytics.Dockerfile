FROM postgres:15-alpine

COPY dockerfile/init-analytics.sql /docker-entrypoint-initdb.d/

EXPOSE 5432