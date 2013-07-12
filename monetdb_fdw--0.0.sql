/* contrib/monetdb_fdw/monetdb_fdw--0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION monetdb_fdw" to load this file. \quit

CREATE FUNCTION monetdb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION monetdb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER monetdb_fdw
  HANDLER monetdb_fdw_handler
  VALIDATOR monetdb_fdw_validator;
