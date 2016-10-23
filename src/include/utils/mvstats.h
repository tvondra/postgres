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

/*
 * Degree of how much MCV item / histogram bucket matches a clause.
 * This is then considered when computing the selectivity.
 */
#define MVSTATS_MATCH_NONE		0		/* no match at all */
#define MVSTATS_MATCH_PARTIAL	1		/* partial match */
#define MVSTATS_MATCH_FULL		2		/* full match */

#define MVSTATS_MAX_DIMENSIONS	8		/* max number of attributes */

#define MVSTAT_NDISTINCT_MAGIC		0xA352BFA4		/* marks serialized bytea */
#define MVSTAT_NDISTINCT_TYPE_BASIC	1		/* basic MCV list type */

/* Multivariate distinct coefficients. */
typedef struct MVNDistinctItem {
	double		ndistinct;
	int			nattrs;
	int		   *attrs;
} MVNDistinctItem;

typedef struct MVNDistinctData {
	uint32			magic;			/* magic constant marker */
	uint32			type;			/* type of ndistinct (BASIC) */
	int				nitems;
	MVNDistinctItem	items[FLEXIBLE_ARRAY_MEMBER];
} MVNDistinctData;

typedef MVNDistinctData *MVNDistinct;


#define MVSTAT_DEPS_MAGIC		0xB4549A2C		/* marks serialized bytea */
#define MVSTAT_DEPS_TYPE_BASIC	1		/* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData
{
	double		degree;			/* degree of validity (0-1) */
	int			nattributes;	/* number of attributes */
	int16		attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData *MVDependency;

typedef struct MVDependenciesData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	int32		ndeps;			/* number of dependencies */
	MVDependency deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependenciesData;

typedef MVDependenciesData *MVDependencies;


/* used to flag stats serialized to bytea */
#define MVSTAT_MCV_MAGIC		0xE1A651C2		/* marks serialized bytea */
#define MVSTAT_MCV_TYPE_BASIC	1				/* basic MCV list type */

/* max items in MCV list (mostly arbitrary number */
#define MVSTAT_MCVLIST_MAX_ITEMS	8192

/*
 * Multivariate MCV (most-common value) lists
 *
 * A straight-forward extension of MCV items - i.e. a list (array) of
 * combinations of attribute values, together with a frequency and
 * null flags.
 */
typedef struct MCVItemData
{
	double		frequency;		/* frequency of this combination */
	bool	   *isnull;			/* lags of NULL values (up to 32 columns) */
	Datum	   *values;			/* variable-length (ndimensions) */
} MCVItemData;

typedef MCVItemData *MCVItem;

/* multivariate MCV list - essentally an array of MCV items */
typedef struct MCVListData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MCV list (BASIC) */
	uint32		ndimensions;	/* number of dimensions */
	uint32		nitems;			/* number of MCV items in the array */
	MCVItem    *items;			/* array of MCV items */
} MCVListData;

typedef MCVListData *MCVList;

bool dependency_implies_attribute(MVDependency dependency, AttrNumber attnum,
								  int16 *attmap);
bool dependency_is_fully_matched(MVDependency dependency, Bitmapset *attnums,
								 int16 *attmap);

/* used to flag stats serialized to bytea */
#define MVSTAT_HIST_MAGIC		0x7F8C5670		/* marks serialized bytea */
#define MVSTAT_HIST_TYPE_BASIC	1				/* basic histogram type */

/* max buckets in a histogram (mostly arbitrary number */
#define MVSTAT_HIST_MAX_BUCKETS 16384

/*
 * Multivariate histograms
 */
typedef struct MVBucketData
{

	/* Frequencies of this bucket. */
	float		ntuples;		/* frequency of tuples tuples */

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
	bool	   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	Datum	   *min;
	bool	   *min_inclusive;

	/* upper boundaries - values and information about the inequalities */
	Datum	   *max;
	bool	   *max_inclusive;

	/* used when building the histogram (not serialized/deserialized) */
	void	   *build_data;

} MVBucketData;

typedef MVBucketData *MVBucket;


typedef struct MVHistogramData
{

	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32		nbuckets;		/* number of buckets (buckets array) */
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

typedef struct MVSerializedBucketData
{

	/* Frequencies of this bucket. */
	float		ntuples;		/* frequency of tuples tuples */

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
	bool	   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	uint16	   *min;
	bool	   *min_inclusive;

	/*
	 * indexes of upper boundaries - values and information about the
	 * inequalities (exclusive vs. inclusive)
	 */
	uint16	   *max;
	bool	   *max_inclusive;

} MVSerializedBucketData;

typedef MVSerializedBucketData *MVSerializedBucket;

typedef struct MVSerializedHistogramData
{

	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */

	/*
	 * keep this the same with MVHistogramData, because of deserialization
	 * (same offset)
	 */
	MVSerializedBucket *buckets;	/* array of buckets */

	/*
	 * serialized boundary values, one array per dimension, deduplicated (the
	 * min/max indexes point into these arrays)
	 */
	int		   *nvalues;
	Datum	  **values;

} MVSerializedHistogramData;

typedef MVSerializedHistogramData *MVSerializedHistogram;


MVNDistinct		load_mv_ndistinct(Oid mvoid);
MVDependencies	load_mv_dependencies(Oid mvoid);
MCVList			load_mv_mcvlist(Oid mvoid);
MVSerializedHistogram load_mv_histogram(Oid mvoid);

bytea *serialize_mv_ndistinct(MVNDistinct ndistinct);
bytea *serialize_mv_dependencies(MVDependencies dependencies);
bytea *serialize_mv_mcvlist(MCVList mcvlist, int2vector *attrs,
							VacAttrStats **stats);
bytea *serialize_mv_histogram(MVHistogram histogram, int2vector *attrs,
							VacAttrStats **stats);

/* deserialization of stats (serialization is private to analyze) */
MVNDistinct deserialize_mv_ndistinct(bytea *data);
MVDependencies deserialize_mv_dependencies(bytea *data);
MCVList deserialize_mv_mcvlist(bytea *data);
MVSerializedHistogram deserialize_mv_histogram(bytea * data);

/*
 * Returns index of the attribute number within the vector (i.e. a
 * dimension within the stats).
 */
int mv_get_index(AttrNumber varattno, int2vector *stakeys);

int2vector *find_mv_attnums(Oid mvoid, Oid *relid);

/* functions for inspecting the statistics */
extern Datum pg_mv_stats_mcvlist_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_mcvlist_items(PG_FUNCTION_ARGS);
extern Datum pg_mv_stats_histogram_info(PG_FUNCTION_ARGS);
extern Datum pg_mv_histogram_buckets(PG_FUNCTION_ARGS);


MVNDistinct build_mv_ndistinct(double totalrows, int numrows, HeapTuple *rows,
							   int2vector *attrs, VacAttrStats **stats);

MVDependencies build_mv_dependencies(int numrows, HeapTuple *rows,
					  int2vector *attrs,
					  VacAttrStats **stats);

MCVList build_mv_mcvlist(int numrows, HeapTuple *rows, int2vector *attrs,
				 VacAttrStats **stats, int *numrows_filtered);

MVHistogram build_mv_histogram(int numrows, HeapTuple *rows, int2vector *attrs,
				   VacAttrStats **stats, int numrows_total);

void build_mv_stats(Relation onerel, double totalrows,
			   int numrows, HeapTuple *rows,
			   int natts, VacAttrStats **vacattrstats);

#ifdef DEBUG_MVHIST
extern void debug_histogram_matches(MVSerializedHistogram mvhist, char *matches);
#endif

#endif
