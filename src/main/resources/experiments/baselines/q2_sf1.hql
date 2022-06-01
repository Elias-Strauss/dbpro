INSERT OVERWRITE DIRECTORY "/data/hive_filtered.csv" ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT  l_partkey, l_linenumber, l_shipdate, l_comment FROM lineitem_sf1 WHERE l_shipdate > '1992-03-15' AND l_shipdate <'1998-03-15'
