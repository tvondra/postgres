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
 */

typedef Pointer ColumnarPage;


typedef struct ColumnInfoData
{
	AttrNumber		attnum;
	int				attlen;
	Oid				atttypid;
	bool			attnulls;
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
	uint32		pd_checksum;	/* checksum */
	uint32		pd_flags;		/* flag bits, see below */
	uint32		pd_nitems;		/* number of items on the page */
	uint16		pd_ncolumns;	/* number of columns on the page */
	uint16		pd_version;		/* version of the page */
	ColumnInfoData	pd_columns[FLEXIBLE_ARRAY_MEMBER]; /* column info array */
} ColumnarPageHeaderData;

typedef ColumnarPageHeaderData *ColumnarPageHeader;


#endif   /* COLSTOREDUMMY_H */
