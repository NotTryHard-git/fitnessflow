FROM postgres:15-alpine

COPY dockerfile/init-attendance.sql /docker-entrypoint-initdb.d/

EXPOSE 5432