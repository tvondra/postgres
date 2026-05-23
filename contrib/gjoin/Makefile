# contrib/auto_explain/Makefile

MODULE_big = gjoin
OBJS = \
	$(WIN32RES) \
	gjoin.o

EXTENSION = gjoin
DATA = gjoin--1.0.0.sql

PGFILEDESC = "gjoin - custom join node"

REGRESS = empty basic basic_3 basic_text basic_multi compare compare_3 compare_text compare_multi large compare_large

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gjoin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
