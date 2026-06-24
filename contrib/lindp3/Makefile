# contrib/lindp/Makefile

MODULE_big = lindp3
OBJS = \
	$(WIN32RES) \
	ikkbz.o \
	lindp.o

PGFILEDESC = "lindp3 - LinDP++ join-order search (search-space linearization)"

REGRESS = lindp3 lindp3_lateral_fail lindp3_linearization_fail

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lindp3
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
