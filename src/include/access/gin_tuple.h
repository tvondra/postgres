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
	Size			keylen;		/* bytes in data for key value */
	int				typlen;		/* typlen for key */
	OffsetNumber	attrnum;
	signed char		category;	/* category: normal or NULL? */
	int				nitems;
	char			data[FLEXIBLE_ARRAY_MEMBER];
} GinTuple;

GinTuple *build_gin_tuple(OffsetNumber attnum, unsigned char category,
						  Datum key, int typlen,
						  ItemPointerData *items, uint32 nitems,
						  Size *len);

int compare_gin_tuples(GinTuple *a, GinTuple *b);

#endif							/* GIN_TUPLE_H */
