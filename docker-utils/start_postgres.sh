
docker run --rm -d --name postgres \
          -e POSTGRES_PASSWORD=postgres \
          -e POSTGRES_DB=goalimpacct \
          -e POSTGRES_USER=postgres \
          -p 5432:5432 \
          -v ${HOME}/Desktop:/mnt postgres

RETRIES=20

until docker exec postgres psql -U postgres -d postgres -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

docker exec postgres psql -f /mnt/dump2.sql -U postgres -n goalimpacct