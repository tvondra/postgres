# contrib/lindp_hyper/Makefile

MODULE_big = lindp_hyper
OBJS = \
	$(WIN32RES) \
	lindp.o

PGFILEDESC = "lindp_hyper - LinDP++ join-order search (search-space linearization)"

REGRESS = lindp_hyper

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lindp_hyper
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
