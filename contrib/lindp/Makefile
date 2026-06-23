# contrib/lindp/Makefile

MODULE_big = lindp
OBJS = \
	$(WIN32RES) \
	lindp.o

PGFILEDESC = "LinDP - LinDP++ join-order search (search-space linearization)"

REGRESS = lindp_basic lindp_lateral_joins_failure lindp_outer_joins_failure \
	lindp_fail_outer_joins lindp_failure lindp_fail_full_outer \
	lindp_fail_lateral lindp_antijoin lindp_lateral lindp_lateral_nested \
	lindp2_fail_lateral lindp2_fail_nested_outer lindp2_fail_outer_joins \
	lindp2_ex1_outer_joins lindp2_ex2_nested_outer lindp2_ex3_lateral \
	lindp2_ex4_right_outer lindp2_ex5_lateral_outer \
	lindp3_lateral_fail lindp3_linearization_fail \
	plan_cost_regress_1 plan_cost_regress_2 \
	subplan_targetlist_failure \
	syn_min_righthand_outer_joins

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lindp
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
