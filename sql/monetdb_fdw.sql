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

CREATE SERVER monetdb_server FOREIGN DATA WRAPPER monetdb_fdw;
CREATE USER MAPPING FOR current_user SERVER monetdb_server;

CREATE FOREIGN TABLE nation (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation ORDER BY 1;

CREATE FOREIGN TABLE nation1 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'nosuchhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation1;

CREATE FOREIGN TABLE nation2 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50001', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation2;

CREATE FOREIGN TABLE nation3 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'nosuchuser', passwd 'monetdb', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation3;

CREATE FOREIGN TABLE nation4 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'wrongpass', dbname 'dbt3', table 'nation')
;

SELECT * FROM nation4;

CREATE FOREIGN TABLE nation5 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'nosuchdb', table 'nation')
;

SELECT * FROM nation5;

CREATE FOREIGN TABLE nation6 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'nosuchtable')
;

SELECT * FROM nation6;

CREATE FOREIGN TABLE nation7 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', table 'select * fro nation')
;

SELECT * FROM nation7;

CREATE FOREIGN TABLE nation8 (
        "n_nationkey" INTEGER,
        "n_name"      CHAR(25),
        "n_regionkey" INTEGER,
        "n_comment"   VARCHAR(152)
) SERVER monetdb_server
OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'dbt3', query 'select * fro nation')
;

SELECT * FROM nation8;

DROP FOREIGN TABLE nation1;
DROP FOREIGN TABLE nation2;
DROP FOREIGN TABLE nation3;
DROP FOREIGN TABLE nation4;
DROP FOREIGN TABLE nation5;
DROP FOREIGN TABLE nation6;
DROP FOREIGN TABLE nation7;
DROP FOREIGN TABLE nation8;

\d
