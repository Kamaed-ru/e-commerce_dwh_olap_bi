# e-commerce_dwh_olap_bi
В этом проекте будет полный цикл преобразований от получения сырых данных в формате JSON по продажам до настройки отчетов в BI системе


ПЕРВЫМ ДЕЛОМ БИЛБИМ ОБРАЗЫ
docker compose build

ПОТОМ РАЗВОРАЧИВАЕМ КОНТЕЙНЕРЫ 
docker compose up -d


airflow - http://localhost:8080/
minio - http://localhost:9001/
spark - http://localhost:8081/
jupyter - http://localhost:8888/
postgres_dwh - localhost:5433

для ребилда
docker build -t spark-ephemeral:3.5.7 ./spark

docker build -t airflow-env:2.11.0 ./airflow
