#!/bin/bash

docker build -t airflow:2.3.0 . --no-cache

# airflow
docker-compose up -d
# spark
docker-compose -f spark-bitnami/docker-compose.yml up -d

