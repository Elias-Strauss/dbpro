set hive.exec.mode.local.auto=false;
INSERT OVERWRITE DIRECTORY "/data/hive_filtered.csv" ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
SELECT rowkey, l_linenumber, l_comment  FROM lineitem_sf100 WHERE l_shipdate > '1992-03-15' AND l_shipdate <'1998-03-15'
