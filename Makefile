MODULE_big = monetdb_fdw
OBJS = monetdb_fdw.o

EXTENSION = monetdb_fdw
DATA = monetdb_fdw--0.0.sql
#DATA_built = monetdb_fdw.sql

REGRESS = monetdb_fdw

SHLIB_LINK = -lmapi
PG_CPPFLAGS = -I/usr/include/monetdb

#EXTRA_CLEAN = sql/monetdb_fdw.sql expected/monetdb_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/monetdb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
