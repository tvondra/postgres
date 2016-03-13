/*-------------------------------------------------------------------------
 *
 * mvstats.h
 *	  Multivariate statistics and selectivity estimation functions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/mvstats.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MVSTATS_H
#define MVSTATS_H

#include "fmgr.h"
#include "commands/vacuum.h"

typedef enum MVStatSearchType
{
	MVSTAT_SEARCH_EXHAUSTIVE,		/* exhaustive search */
	MVSTAT_SEARCH_GREEDY			/* greedy search */
}	MVStatSearchType;

extern int mvstat_search_type;

/*
 * Degree of how much MCV item / histogram bucket matches a clause.
 * This is then considered when computing the selectivity.
 */
#define MVSTATS_MATCH_NONE		0		/* no match at all */
#define MVSTATS_MATCH_PARTIAL	1		/* partial match */
#define MVSTATS_MATCH_FULL		2		/* full match */

#define MVSTATS_MAX_DIMENSIONS	8		/* max number of attributes */


/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData {
	int 	nattributes;	/* number of attributes */
	int16	attributes[1];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData* MVDependency;

typedef struct MVDependenciesData {
	uint32			magic;		/* magic constant marker */
	uint32			type;		/* type of MV Dependencies (BASIC) */
	int32			ndeps;		/* number of dependencies */
	MVDependency	deps[1];	/* XXX why not a pointer? */
} MVDependenciesData;

typedef MVDependenciesData* MVDependencies;

#define MVSTAT_DEPS_MAGIC		0xB4549A2C	/* marks serialized bytea */
#define MVSTAT_DEPS_TYPE_BASIC	1			/* basic dependencies type */

/*
 * Multivariate MCV (most-common value) lists
 *
 * A straight-forward extension of MCV items - i.e. a list (array) of
 * combinations of attribute values, together with a frequency and
 * null flags.
 */
typedef struct MCVItemData {
	double		frequency;	/* frequency of this combination */
	bool	   *isnull;		/* lags of NULL values (up to 32 columns) */
	Datum	   *values;		/* variable-length (ndimensions) */
} MCVItemData;

typedef MCVItemData *MCVItem;

/* multivariate MCV list - essentally an array of MCV items */
typedef struct MCVListData {
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MCV list (BASIC) */
	uint32		ndimensions;	/* number of dimensions */
	uint32		nitems;			/* number of MCV items in the array */
	MCVItem	   *items;			/* array of MCV items */
} MCVListData;

typedef MCVListData *MCVList;

/* used to flag stats serialized to bytea */
#define MVSTAT_MCV_MAGIC		0xE1A651C2	/* marks serialized bytea */
#define MVSTAT_MCV_TYPE_BASIC	1			/* basic MCV list type */

/*
 * Limits used for mcv_max_items option, i.e. we're always guaranteed
 * to have space for at least MVSTAT_MCVLIST_MIN_ITEMS, and we cannot
 * have more than MVSTAT_MCVLIST_MAX_ITEMS items.
 *
 * This is just a boundary for the 'max' threshold - the actual list
 * may of course contain less items than MVSTAT_MCVLIST_MIN_ITEMS.
 */
#define MVSTAT_MCVLIST_MIN_ITEMS	128		/* min items in MCV list */
#define MVSTAT_MCVLIST_MAX_ITEMS	8192	/* max items in MCV list */

/*
 * Multivariate histograms
 */
typedef struct MVBucketData {

	/* Frequencies of this bucket. */
	float	ntuples;	/* frequency of tuples tuples */

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
	bool   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	Datum  *min;
	bool   *min_inclusive;

	/* upper boundaries - values and information about the inequalities */
	Datum  *max;
	bool   *max_inclusive;

	/* used when building the histogram (not serialized/deserialized) */
	void   *build_data;

} MVBucketData;

typedef MVBucketData	*MVBucket;


typedef struct MVHistogramData {

	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32 		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */

	MVBucket   *buckets;		/* array of buckets */

} MVHistogramData;

typedef MVHistogramData *MVHistogram;

/*
 * Histogram in a partially serialized form, with deduplicated boundary
 * values etc.
 *
 * TODO add more detailed description here
 */

typedef struct MVSerializedBucketData {

	/* Frequencies of this bucket. */
	float	ntuples;	/* frequency of tuples tuples */

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
	bool   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	uint16 *min;
	bool   *min_inclusive;

	/* indexes of upper boundaries - values and information about the
	 * inequalities (exclusive vs. inclusive) */
	uint16 *max;
	bool   *max_inclusive;

} MVSerializedBucketData;

typedef MVSerializedBucketData	*MVSerializedBucket;

typedef struct MVSerializedHistogramData {

	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32 		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */

	/*
	 * keep this the same with MVHistogramData, because of
	 * deserialization (same offset)
	 */
	MVSerializedBucket   *buckets;		/* array of buckets */

	/*
	 * serialized boundary values, one array per dimension, deduplicated
	 * (the min/max indexes point into these arrays)
	 */
	int	   *nvalues;
	Datum **values;

} MVSerializedHistogramData;

typedef MVSerializedHistogramData *MVSerializedHistogram;


/* used to flag stats serialized to bytea */
#define MVSTAT_HIST_MAGIC		0x7F8C5670	/* marks serialized bytea */
#define MVSTAT_HIST_TYPE_BASIC	1			/* basic histogram type */

/*
 * Limits used for max_buckets option, i.e. we're always guaranteed
 * to have space for at least MVSTAT_HIST_MIN_BUCKETS, and we cannot
 * have more than MVSTAT_HIST_MAX_BUCKETS buckets.
 *
 * This is just a boundary for the 'max' threshold - the actual
 * histogram may use less buckets than MVSTAT_HIST_MAX_BUCKETS.
 *
 * TODO The MVSTAT_HIST_MIN_BUCKETS should be related to the number of
 *      attributes (MVSTATS_MAX_DIMENSIONS) because of NULL-buckets.
 *      There should be at least 2^N buckets, otherwise we may be unable
 *      to build the NULL buckets.
 */
#define MVSTAT_HIST_MIN_BUCKETS	128			/* min number of buckets */
#define MVSTAT_HIST_MAX_BUCKETS	16384		/* max number of buckets */

/*
 * TODO Maybe fetching the histogram/MCV list separately is inefficient?
 *      Consider adding a single `fetch_stats` method, fetching all
 *      stats specified using flags (or something like that).
 */

MVDependencies load_mv_dependencies(Oid mvoid);
MCVList        load_mv_mcvlist(Oid mvoid);
MVSerializedHistogram    load_mv_histogram(Oid mvoid);
double		   load_mv_ndistinct(Oid mvoid);

bytea * serialize_mv_dependencies(MVDependencies dependencies);
bytea * serialize_mv_mcvlist(MCVList mcvlist, int2vector *attrs,
							 VacAttrStats **stats);
bytea * serialize_mv_histogram(MVHistogram histogram, int2vector *attrs,
					  VacAttrStats **stats);

/* deserialization of stats (serialization is private to analyze) */
MVDependencies	deserialize_mv_dependencies(bytea * data);
MCVList			deserialize_mv_mcvlist(bytea * data);
MVSerializedHistogram	deserialize_mv_histogram(bytea * data);

/*
 * Returns index of the attribute number within the vector (i.e. a
 * dimension within the stats).
 */
int mv_get_index(AttrNumber varattno, int2vector * stakeys);
int2vector* find_mv_attnums(Oid mvoid, Oid *relid);

int2vector* find_mv_attnums(Oid mvoid, Oid *relid);

/* FIXME this probably belongs somewhere else (not to operations stats) */
extern Datum pg_mv_stats_dependencies_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_stats_dependencies_show(PG_FUNCTION_ARGS);
extern Datum pg_mv_stats_mcvlist_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_mcvlist_items(PG_FUNCTION_ARGS);
extern Datum pg_mv_stats_histogram_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_histogram_buckets(PG_FUNCTION_ARGS);

MVDependencies
build_mv_dependencies(int numrows, HeapTuple *rows, int2vector *attrs,
					  VacAttrStats **stats);

MCVList
build_mv_mcvlist(int numrows, HeapTuple *rows, int2vector *attrs,
				 VacAttrStats **stats, int *numrows_filtered);

MVHistogram
build_mv_histogram(int numrows, HeapTuple *rows, int2vector *attrs,
				   VacAttrStats **stats, int numrows_total);

double
build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
				   int2vector *attrs, VacAttrStats **stats);

void build_mv_stats(Relation onerel, double totalrows,
					int numrows, HeapTuple *rows,
					int natts, VacAttrStats **vacattrstats);

void update_mv_stats(Oid relid, MVDependencies dependencies,
					 MCVList mcvlist, MVHistogram histogram,
					 double ndistcoeff,
					 int2vector *attrs, VacAttrStats **stats);

#ifdef DEBUG_MVHIST
extern void debug_histogram_matches(MVSerializedHistogram mvhist, char *matches);
#endif


#endif
