#!/bin/sh

PSQL_OPTS=""

if [ -z "$PGHOME" ]; then
  PGHOME=/tmp/pgsql
fi

USE_PGXS=1
PATH=${PGHOME}/bin:$PATH
export PGHOME USE_PGXS PATH

make clean USE_PGXS=1
make all USE_PGXS=1
make install USE_PGXS=1

DBNAME=__fdwtest__

echo dropdb ${PSQL_OPTS} ${DBNAME}
dropdb ${PSQL_OPTS} ${DBNAME}
createdb ${PSQL_OPTS} ${DBNAME}
#psql ${PSQL_OPTS} -f monetdb_fdw.sql ${DBNAME}

psql ${PSQL_OPTS} ${DBNAME} <<EOF
CREATE FUNCTION monetdb_fdw_handler()
RETURNS fdw_handler
AS 'monetdb_fdw'
LANGUAGE C STRICT;

CREATE FUNCTION monetdb_fdw_validator(text[], oid)
RETURNS void
AS 'monetdb_fdw'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER monetdb_fdw
  HANDLER monetdb_fdw_handler
  VALIDATOR monetdb_fdw_validator;
EOF

psql ${PSQL_OPTS} ${DBNAME} <<EOF
\timing

CREATE SERVER monetdb_server FOREIGN DATA WRAPPER monetdb_fdw;

CREATE USER MAPPING FOR current_user SERVER monetdb_server;

--
-- customer
--
CREATE FOREIGN TABLE nation (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation;

--
-- customer
--
CREATE FOREIGN TABLE customer (
        count      text
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', query 'select count(*) from customer')
;

SELECT * FROM customer;

EXPLAIN SELECT * FROM customer;
EXPLAIN ANALYZE SELECT * FROM customer;
ANALYZE customer;

CREATE FOREIGN TABLE q12 (
        l_shipmode text,
        high_line_count bigint,
        low_line_count bigint
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', query '
select l_shipmode,
        sum(case when o_orderpriority = ''1-URGENT'' or o_orderpriority = ''2-HIGH'' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> ''1-URGENT'' and o_orderpriority <> ''2-HIGH'' then 1 else 0 end) as low_line_count
   from orders, lineitem
  where o_orderkey = l_orderkey and l_shipmode in (''TRUCK'', ''REG AIR'')
    and l_commitdate < l_receiptdate and l_shipdate < l_commitdate
    and l_receiptdate >= date ''1994-01-01''
    and l_receiptdate < date ''1995-01-01''
  group by l_shipmode
  order by l_shipmode;
'
);

SELECT * FROM q12;

EOF
