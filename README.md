monetdb_fdw
===========

Introduction
------------

This PostgreSQL extension implements
a [Foreign Data Wrapper (FDW)](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)
for [MonetDB](https://www.monetdb.org/Home).


Building
------------

To compile the MonetDB foreign data wrapper,
MonetDB [MAPI library](https://www.monetdb.org/Documentation/Manuals/SQLreference/Programming/MAPI) is needed.
So please install MonetDB client interface library development files.

    # Fedora, CentOS
    sudo yum install MonetDB-client-devel

    # Debian and Ubuntu
    sudo apt-get install libmonetdb-client-dev

If your MonetDB MAPI library is in a non-standard location, you need update
monetdb_fdw Makefile. For example,

    SHLIB_LINK = -L/usr/local/monetdb/lib64 -lmapi
    PG_CPPFLAGS = -I/usr/local/monetdb/include/monetdb

And Depends on your environment maybe needs to setup PostgreSQL PATH environment
variable (if shell is bash):

    # CentOS
    export PATH=/usr/pgsql-9.4/bin:$PATH
    
Then into the monetdb_fdw development folder, compile and install the code
using make:

    make USE_PGXS=1
    sudo make USE_PGXS=1 install


Usage
------------

Before using monetdb_fdw, you need to add it to shared_preload_libraries in
your `postgresql.conf` and restart PostgreSQL server:

    shared_preload_libraries = 'monetdb_fdw'


Example
------------

Using `psql` client to verify.

Load monetdb_fdw extension first time after install:

    CREATE EXTENSION monetdb_fdw;

Create a server object:

    CREATE SERVER monetdb_server FOREIGN DATA WRAPPER monetdb_fdw;

If your MonetDB database "demo" has a table name "persion",
you can create a foreign table like this:

    CREATE FOREIGN TABLE person (id integer, name varchar(40)) SERVER monetdb_server
    OPTIONS (host 'localhost', port '50000', user 'monetdb', passwd 'monetdb', dbname 'demo', table 'person');

Next, try to query person table:

    SELECT * FROM person;


Uninstalling
------------

First you need to drop all the monetdb_fdw tables before uninstalling the extension:

    DROP FOREIGN TABLE person;

Then drop the monetdb server and extension:

    DROP SERVER monetdb_server;
    DROP EXTENSION monetdb_fdw;

Then remove monetdb_fdw from shared_preload_libraries in your postgresql.conf:

    #shared_preload_libraries = 'monetdb_fdw'

Finally remove the monetdb_fdw files, into the monetdb_fdw development folder,
execute:

    sudo make USE_PGXS=1 uninstall


