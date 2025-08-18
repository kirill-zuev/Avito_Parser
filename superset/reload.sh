docker-compose -f docker-compose-superset.yml down

docker-compose -f docker-compose-superset.yml up -d

# docker exec -it superset superset db upgrade

# docker exec -it superset superset fab create-admin

# docker exec -it superset superset init

# docker exec -it superset pip install psycopg2-binary