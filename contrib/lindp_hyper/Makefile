# contrib/lindp_hyper/Makefile

MODULE_big = lindp_hyper
OBJS = \
	$(WIN32RES) \
	lindp.o

PGFILEDESC = "lindp_hyper - LinDP++ join-order search (search-space linearization)"

REGRESS = lindp_hyper lindp_lateral_joins_failure lindp_outer_joins_failure \
	lindp_fail_outer_joins lindp_failure lindp_fail_full_outer \
	lindp_fail_lateral lindp_antijoin lindp_lateral lindp_lateral_nested \
	lindp2_fail_lateral lindp2_fail_nested_outer lindp2_fail_outer_joins \
	lindp2_ex1_outer_joins lindp2_ex2_nested_outer lindp2_ex3_lateral \
	lindp2_ex4_right_outer lindp2_ex5_lateral_outer

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
