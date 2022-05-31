#!/bin/bash
echo $1

set -x
SECONDS=0

docker cp q1_sf1.hql hive-server:/
docker cp q1_sf10.hql hive-server:/
docker cp q1_sf100.hql hive-server:/

docker exec hive-server bash -c "hive -f q1_sf$1.hql"

docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
  --class "hdfs.csv.Filter" \
  --master "spark://spark-master:7077"\
  --executor-memory 30G  \
  --conf "spark.hadoop.fs.defaultFS"="hdfs://namenode:9000"\
  sparkio.jar\
  hdfs:///data/partsupp_sf$1.tbl hdfs:///data/spark_filtered.csv"

docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
  --class "hdfs.csv.JoinProjectedUC1"  \
  --master "spark://spark-master:7077" \
  --executor-memory 30G  \
  --conf "spark.hadoop.fs.defaultFS"="hdfs://namenode:9000" \
  sparkio.jar  hdfs:///data/hive_filtered.csv hdfs:///data/spark_filtered.csv hdfs:///data/spark_joined.csv"

docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
  --class "jdbc.WriteJoined"  \
  --master "spark://spark-master:7077" \
  --executor-memory 30G  \
  --conf "spark.hadoop.fs.defaultFS"="hdfs://namenode:9000" \
  --jars postgresql-42.2.18.jar \
  sparkio.jar  hdfs:///data/spark_joined.csv postgres-db:5432/polydb polydb polydbPW123 sink"

duration=$SECONDS
echo "$duration seconds elapsed."
