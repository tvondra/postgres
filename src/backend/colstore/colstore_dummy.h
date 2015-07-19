/*-------------------------------------------------------------------------
 *
 * colstoreapi.h
 *	  API for column store implementations
 *
 * Copyright (c) 2010-2015, PostgreSQL Global Development Group
 *
 * src/include/colstore/colstoreapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COLSTOREDUMMY_H
#define COLSTOREDUMMY_H

#include "access/attnum.h"
#include "storage/bufpage.h"
#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/item.h"
#include "storage/itemptr.h"
#include "storage/off.h"

/*
 * A columnar disk page is an abstraction layered on top of a postgres
 * disk block (which is simply a unit of i/o, see block.h).
 *
 * specifically, while a disk block can be unformatted, a columnar disk
 * page format depends on the particular column store implementation.
 * For the 'dummy' implementation, it is a slotted page of the form:
 *
 * +----------------+-------+-----------------+-----------------+
 * | ColumnarPageHeaderData | ColumnInfoData1 | ColumnInfoData2 |
 * +-------+----------------+-+---------------+-----------------+
 * |  ...   ColumnInfoDataN   |           tuple IDs             |
 * +---------------------+----+----------------+----------------+
 * |    column 1 data    |    column 2 data    |      ....      |
 * +---------------------+---------------------+----------------+
 * |                                                            |
 * |                                                            |
 * +---------------------------------+--------------------------+
 * |        ...                      |       column N data      |
 * +---------------------------------+--------------------------+
 *
 * a page is full when a new tuple can't be added (even after moving
 * the data around, compressing etc.)
 *
 * all blocks written out by an access method must be disk pages.
 *
 * EXCEPTIONS:
 *
 * obviously, a page is not formatted before it is initialized by
 * a call to PageInit.
 *
 * NOTES:
 *
 * The tuple IDs contain tuple IDs for all tuples stored on this page,
 * providing a mapping to the heap part. It's good to keep this array
 * sorted, as that makes lookup faster. It's also possible to encode
 * this array using RLE, for example (again, that works better for
 * sorted data). There's also a min/max TID in the page header.
 *
 * The 'column data' combine all the data for a column, i.e. the actual
 * values and NULL bitmap. The data may be partially compressed, etc.
 *
 * Some of the page fields may seem too big (e.g. 32 bits for nitems seems
 * a bit over the top, but (a) 16 bits is just on the border for 64kB pages
 * (and larger pages may get supported in the future), (b) we do expect
 * efficient storage of some data types (e.g. bool type in 1 bit). That makes
 * the 16bit data type inadequate.
 *
 * We must however keep the beginning of the header exactly the same as for
 * regular pages, so that the checksum / validation stuff works.
 */

typedef Pointer ColumnarPage;

typedef struct ColumnInfoData
{
	AttrNumber		attnum;
	int				attlen;
	Oid				atttypid;
	bool			attnotnull;
	LocationIndex	data_start;
	LocationIndex	data_bytes;
	LocationIndex	nulls_start;
	LocationIndex	nulls_bytes;
} ColumnInfoData;

typedef struct ColumnarPageHeaderData
{
	/* XXX LSN is member of *any* block, not only page-organized ones */
	PageXLogRecPtr	pd_lsn;		/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_checksum;	/* checksum */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;

	/* our fields start here */
	LocationIndex pd_tupleids;	/* offset of tuple IDs */
	uint16		pd_ncolumns;	/* number of columns on the page */
	uint32		pd_nitems;		/* number of items on the page */
	uint32		pd_maxitems;	/* max number of items on the page */
	ItemPointerData	pd_min_tid;	/* mininum TID placed on page */
	ItemPointerData pd_max_tid;	/* maximum TID placed on page */
	ColumnInfoData	pd_columns[FLEXIBLE_ARRAY_MEMBER]; /* column info array */
} ColumnarPageHeaderData;

typedef ColumnarPageHeaderData *ColumnarPageHeader;


#endif   /* COLSTOREDUMMY_H */
