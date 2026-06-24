# contrib/lindp2/Makefile

MODULE_big = lindp2
OBJS = \
	$(WIN32RES) \
	ikkbz.o \
	joingraph.o \
	lindp.o

PGFILEDESC = "lindp2 - linearized (IKKBZ + LinDP++) join search"

REGRESS = lindp2 lindp2_fail_lateral lindp2_fail_nested_outer lindp2_fail_outer_joins lindp2_ex1_outer_joins lindp2_ex2_nested_outer lindp2_ex3_lateral lindp2_ex4_right_outer lindp2_ex5_lateral_outer

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lindp2
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
