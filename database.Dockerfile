FROM postgres:13-alpine

COPY ./dataline-db/src/main/resources/schema.sql /docker-entrypoint-initdb.d/000_init.sql
