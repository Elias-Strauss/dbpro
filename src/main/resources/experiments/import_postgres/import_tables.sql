/*CREATE TABLE orders_sf10
(
    o_orderkey      VARCHAR,
    o_custkey       VARCHAR,
    o_orderstatus   VARCHAR,
    o_totalprice    VARCHAR,
    o_orderdate     VARCHAR,
    o_orderpriority VARCHAR,
    o_clerk         VARCHAR,
    o_shippriority  VARCHAR,
    o_comment       VARCHAR
);
CREATE TABLE orders_sf1
(
    o_orderkey      VARCHAR,
    o_custkey       VARCHAR,
    o_orderstatus   VARCHAR,
    o_totalprice    VARCHAR,
    o_orderdate     VARCHAR,
    o_orderpriority VARCHAR,
    o_clerk         VARCHAR,
    o_shippriority  VARCHAR,
    o_comment       VARCHAR
);*/

CREATE TABLE partsupp_sf10
(
    ps_partkey    VARCHAR,
    ps_suppkey    VARCHAR,
    ps_availqty   VARCHAR,
    ps_supplycost VARCHAR,
    ps_comment    VARCHAR
);
CREATE TABLE partsupp_sf1
(
    ps_partkey    VARCHAR,
    ps_suppkey    VARCHAR,
    ps_availqty   VARCHAR,
    ps_supplycost VARCHAR,
    ps_comment    VARCHAR
);

--copy orders_sf1 FROM '/tmp/orders_sf1.tbl' with CSV DELIMITER '|' QUOTE '"' ESCAPE '\';
--copy orders_sf10 FROM '/tmp/orders_sf10.tbl' with CSV DELIMITER '|' QUOTE '"' ESCAPE '\';
copy partsupp_sf1 FROM '/tmp/partsupp_sf1.tbl' with CSV DELIMITER '|' QUOTE '"' ESCAPE '\';
copy partsupp_sf10 FROM '/tmp/partsupp_sf10.tbl' with CSV DELIMITER '|' QUOTE '"' ESCAPE '\';
