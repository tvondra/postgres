/*-------------------------------------------------------------------------
 *
 * ams.h
 *	  Declarations for AMS sketch.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ams.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AMS_H
#define AMS_H

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			count;			/* number of items added */
	int			nrows;			/* # of rows of counters */
	int			ncounters;		/* # of counters per row */
	int			counters[FLEXIBLE_ARRAY_MEMBER];	/* counters */
} AMSSketch;

#define	AMS_DEFAULT_ROWS	10
#define	AMS_DEFAULT_COLS	20

AMSSketch *AMSSketchInit(int nrows, int ncounters);
void AMSSketchAddValue(AMSSketch *sketch, const void *val, int len);

#endif							/* AMS_H */
