# contrib/scrub/Makefile

MODULE_big = scrub
OBJS = scrub.o scrub_checks.o $(WIN32RES)

EXTENSION = scrub
DATA = scrub--1.0.sql
PGFILEDESC = "scrub - check cluster consistency"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/scrub
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
