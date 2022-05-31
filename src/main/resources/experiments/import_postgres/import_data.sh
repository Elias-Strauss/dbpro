#!/bin/bash
echo $1

set -x
docker cp namenode:/data/partsupp_sf1.tbl .
docker cp namenode:/data/partsupp_sf10.tbl .
docker cp partsupp_sf10.tbl postgres-db:/tmp/
docker cp partsupp_sf1.tbl postgres-db:/tmp/

docker cp namenode:/data/orders_sf1.tbl .
docker cp namenode:/data/orders_sf10.tbl .
docker cp orders_sf10.tbl postgres-db:/tmp/
docker cp orders_sf1.tbl postgres-db:/tmp/
docker cp import_tables.sql postgres-db:/
docker exec -it --user postgres postgres-db bash -c "psql -U polydb -d polydb -f import_tables.sql"
