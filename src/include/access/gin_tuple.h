/*--------------------------------------------------------------------------
 * gin.h
 *	  Public header file for Generalized Inverted Index access method.
 *
 *	Copyright (c) 2006-2024, PostgreSQL Global Development Group
 *
 *	src/include/access/gin.h
 *--------------------------------------------------------------------------
 */
#ifndef GIN_TUPLE_
#define GIN_TUPLE_


typedef struct GinTuple
{
	Size			tuplen;		/* length of the whole tuple */
	Size			keylen;		/* bytes in data for key value */
	int16			typlen;		/* typlen for key */
	bool			typbyval;	/* typbyval for key */
	OffsetNumber	attrnum;
	signed char		category;	/* category: normal or NULL? */
	int				nitems;		/* number of TIDs in the data */
	char			data[FLEXIBLE_ARRAY_MEMBER];
} GinTuple;

extern GinTuple *_gin_build_tuple(OffsetNumber attnum, unsigned char category,
								  Datum key, int16 typlen, bool typbyval,
								  ItemPointerData *items, uint32 nitems,
								  Size *len);

extern int _gin_compare_tuples(GinTuple *a, GinTuple *b);

#endif							/* GIN_TUPLE_H */
