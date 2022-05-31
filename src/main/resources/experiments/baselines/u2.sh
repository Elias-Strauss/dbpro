#!/bin/bash
echo $1

set -x
SECONDS=0

docker cp q2_sf1.hql hive-server:/
docker cp q2_sf10.hql hive-server:/
docker cp q2_sf100.hql hive-server:/

docker exec hive-server bash -c "hive -f q2_sf$1.hql"

docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
  --class "hdfs.csv.Filter" \
  --master "spark://spark-master:7077"\
  --executor-memory 30G  \
  --conf "spark.hadoop.fs.defaultFS"="hdfs://namenode:9000"\
  sparkio.jar\
  hdfs:///data/partsupp_sf$1.tbl hdfs:///data/spark_filtered.csv"

docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
  --class "hdfs.csv.JoinUDF"  \
  --master "spark://spark-master:7077" \
  --executor-memory 30G  \
  --conf "spark.hadoop.fs.defaultFS"="hdfs://namenode:9000" \
  sparkio.jar  hdfs:///data/hive_filtered.csv hdfs:///data/spark_filtered.csv hdfs:///data/spark_joined.csv"

duration=$SECONDS
echo "$duration seconds elapsed."
