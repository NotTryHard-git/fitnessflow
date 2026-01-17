FROM postgres:15-alpine

COPY dockerfile/init-booking.sql /docker-entrypoint-initdb.d/

EXPOSE 5432