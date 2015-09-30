/*-------------------------------------------------------------------------
 *
 * columnar.h
 *	  header file for postgres columnar access method implementation.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/columnar.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COLUMNAR_H
#define COLUMNAR_H

#include "access/genam.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"

/*
 * Columnar indexes only have data pages at this point. Eventually they might
 * get more complicated - with meta page and possibly some additional page
 * types (e.g. mapping TID ranges to pages), but now it's just a simple
 * sequence of data pages.
 *
 * All the fields are currently uint32, mostly because I'm lazy to check what
 * minimum size would be sufficient, and because the difference does not matter
 * in the bigger scheme of things.
 */
typedef struct ColumnarPageOpaqueData
{
	uint32		colpo_magic;		/* should contain COLUMNAR_MAGIC */
	uint32		colpo_version;		/* should contain COLUMNAR_VERSION */

	uint32		colpo_flags;		/* flags */
	uint32		colpo_nitems;		/* number of items on the page */
} ColumnarPageOpaqueData;

typedef ColumnarPageOpaqueData *ColumnarPageOpaque;

#define COLUMNAR_MAGIC		0x071237	/* magic number of colmap pages */
#define COLUMNAR_VERSION	1			/* current version number */

#define COLP_FULL				(0x01 << 0)	/* data page is full */
#define COLP_NO_RECOMPRESS		(0x01 << 1)	/* recompression disabled */
#define COLP_NO_REDISTRIBUTE	(0x01 << 2)	/* redistribtion disabled */

/*
 * Maximum size of a columnar index entry, including its tuple header.
 *
 * We actually need to be able to fit a single item on every page,
 * so restrict any one item to the per-page available space.
 */
#define ColumnarMaxItemSize(page) \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData + sizeof(ItemPointerData)) - \
				   MAXALIGN(sizeof(ColumnarPageOpaqueData))))

#define COL_READ			BUFFER_LOCK_SHARE
#define COL_WRITE			BUFFER_LOCK_EXCLUSIVE


/*
 * BTScanOpaqueData is the btree-private state needed for an indexscan.
 * This consists of preprocessed scan keys (see _bt_preprocess_keys() for
 * details of the preprocessing), information about the current location
 * of the scan, and information about the marked location, if any.  (We use
 * BTScanPosData to represent the data needed for each of current and marked
 * locations.)	In addition we can remember some known-killed index entries
 * that must be marked before we can move off the current page.
 *
 * Index scans work a page at a time: we pin and read-lock the page, identify
 * all the matching items on the page and save them in BTScanPosData, then
 * release the read-lock while returning the items to the caller for
 * processing.  This approach minimizes lock/unlock traffic.  Note that we
 * keep the pin on the index page until the caller is done with all the items
 * (this is needed for VACUUM synchronization, see nbtree/README).  When we
 * are ready to step to the next page, if the caller has told us any of the
 * items were killed, we re-lock the page to mark them killed, then unlock.
 * Finally we drop the pin and step to the next page in the appropriate
 * direction.
 *
 * If we are doing an index-only scan, we save the entire IndexTuple for each
 * matched item, otherwise only its heap TID and offset.  The IndexTuples go
 * into a separate workspace array; each BTScanPosItem stores its tuple's
 * offset within that array.
 */

typedef struct ColumnScanPosItem	/* what we remember about each match */
{
	ItemPointerData heapTid;	/* TID of referenced heap item */
	OffsetNumber indexOffset;	/* index item's location within page */
	LocationIndex tupleOffset;	/* IndexTuple's offset in workspace, if any */
} ColumnScanPosItem;

typedef struct ColumnScanPosData
{
	Buffer		buf;			/* if valid, the buffer is pinned */

	XLogRecPtr	lsn;			/* pos in the WAL stream when page was read */
	BlockNumber currPage;		/* page we've referencd by items array */
	BlockNumber nextPage;		/* page's right link when we scanned it */

	/*
	 * moreLeft and moreRight track whether we think there may be matching
	 * index entries to the left and right of the current page, respectively.
	 * We can clear the appropriate one of these flags when _bt_checkkeys()
	 * returns continuescan = false.
	 */
	bool		moreLeft;
	bool		moreRight;

	/*
	 * If we are doing an index-only scan, nextTupleOffset is the first free
	 * location in the associated tuple storage workspace.
	 */
	int			nextTupleOffset;

	/*
	 * The items array is always ordered in index order (ie, increasing
	 * indexoffset).  When scanning backwards it is convenient to fill the
	 * array back-to-front, so we start at the last slot and fill downwards.
	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
	 * itemIndex is a cursor showing which entry was last returned to caller.
	 */
	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* current index in items[] */

	ColumnScanPosItem items[MaxIndexTuplesPerPage]; /* MUST BE LAST */
} ColumnScanPosData;

typedef ColumnScanPosData *ColumnScanPos;

/* We need one of these for each equality-type SK_SEARCHARRAY scan key */
typedef struct ColumnArrayKeyInfo
{
	int			scan_key;		/* index of associated key in arrayKeyData */
	int			cur_elem;		/* index of current element in elem_values */
	int			mark_elem;		/* index of marked element in elem_values */
	int			num_elems;		/* number of elems in current array value */
	Datum	   *elem_values;	/* array of num_elems Datums */
} ColumnArrayKeyInfo;

typedef struct ColumnScanOpaqueData
{
	/* current / max data page */
	BlockNumber	cur_page;
	BlockNumber	max_page;

	/* these fields are set by _col_preprocess_keys(): */
	bool		qual_ok;		/* false if qual can never be satisfied */
	int			numberOfKeys;	/* number of preprocessed scan keys */
	ScanKey		keyData;		/* array of preprocessed scan keys */

	/* temporary buffer, keeping */
	int			num_items;	/* number of items in the buffer */
	int			cur_item;	/* index of next item to return */

	MemoryContext	tmpctx;	/* temporary context used when reading page */
	MemoryContext	bufctx;	/* context tracking data for current tuples */

	IndexTuple *tuples;		/* already-formed index tuples */

} ColumnScanOpaqueData;

typedef ColumnScanOpaqueData *ColumnScanOpaque;

/* internal structure of a page (needs to be here because of pageinspect) */

/*
 * Pages of a columnar index is split into several contiguous segments,
 * each keeping data for one column or TIDs for the row. The segment
 * keeping TIDs is always the first one on the page, then segments for
 * columns (in the order as defined in the index).
 *
 * Each segment is split into a header (PageSegmentData) and two main
 * parts - one keeping data, one keeping NULL bitmap. The data start
 * at the beginning, bitmap starts at from the end and grows in the
 * opposite direction. When the two parts meet, the segment is full.
 *
 * If the column is NOT NULL, the corresponding segment may be marked
 * as SEG_NULLS_DISABLED and stored without a NULL bitmap. The size
 * of the NULL bitmap is not currently tracked and the bitmap is not
 * compressed in any way - it's simply a binary string, whose size is
 * derived from seg_nitems.
 *
 * The complete length of the segment is stored in seg_len field. The
 * size of the 'data' array (keeping both data and NULL bitmap) is
 * therefore (seg_len - offsetof(PageSegmentData,data)).
 *
 * The space actually occupied by data is tracked in seg_used_len. The
 * data may be partially compressed, splitting the 'used' part into
 * compressed and plain portions. The amount of data compressed is
 * tracked in seg_comp_len (always <= seg_used_len).
 *
 * ------------------------------------------------------------------
 * | header | compressed | plain -> |    empty     | <- null bitmap |
 * ------------------------------------------------------------------
 *                       ^          ^                               ^
 *                 comp_len     used_len                           len
 *
 * Each segment also stores the number of values stored in it - this
 * might be unnecessary as this should be the same for all segments
 * on a page (and we track it in ColumnarPageOpaqueData), but let's
 * keep it for now as having local counter makes some operations easier.
 */
typedef struct PageSegmentData {

	uint16	seg_len;			/* total size of the buffer page (bytes) */
	uint16	seg_used_len;		/* used portion of the buffer (bytes) */
	uint16	seg_comp_len;		/* compressed portion (bytes) */
	int16	seg_typ_len;		/* size of values (pg_type.typlen) */

	uint32	seg_raw_len;		/* total size of data (after decompression) */

	uint32	seg_nitems;			/* number of items stored in the buffer */
	uint32	seg_flags;			/* various buffer flags */

	char	data[1];			/* var-sized buffer data */

} PageSegmentData;

typedef PageSegmentData* PageSegment;

#define	SEG_ITEM_POINTERS		(0x01 << 0)
#define	SEG_DATA				(0x01 << 1)
#define	SEG_NULLS_DISABLED		(0x01 << 2)
#define	SEG_COMPRESS_DISABLED	(0x01 << 3)
#define	SEG_COMPRESS_AUTO		(0x01 << 4)
#define	SEG_COMPRESS_RLE		(0x01 << 5)
#define	SEG_COMPRESS_DICT		(0x01 << 6)
#define	SEG_COMPRESS_DELTA_RLE	(0x01 << 7)
#define	SEG_COMPRESS_PGLZ		(0x01 << 8)


/*
 * prototypes for functions in columnar.c (external entry points for columnar)
 */
extern Datum colbuild(PG_FUNCTION_ARGS);
extern Datum colbuildempty(PG_FUNCTION_ARGS);
extern Datum colinsert(PG_FUNCTION_ARGS);
extern Datum colbeginscan(PG_FUNCTION_ARGS);
extern Datum colgettuple(PG_FUNCTION_ARGS);
extern Datum colgetbitmap(PG_FUNCTION_ARGS);
extern Datum colrescan(PG_FUNCTION_ARGS);
extern Datum colendscan(PG_FUNCTION_ARGS);
extern Datum colmarkpos(PG_FUNCTION_ARGS);
extern Datum colrestrpos(PG_FUNCTION_ARGS);
extern Datum colbulkdelete(PG_FUNCTION_ARGS);
extern Datum colvacuumcleanup(PG_FUNCTION_ARGS);
extern Datum colcanreturn(PG_FUNCTION_ARGS);
extern Datum coloptions(PG_FUNCTION_ARGS);

/* colpage.c */
void _col_initdatapage(Relation index, Page page);

/* colinsert.c */
bool _col_doinsert(Relation rel, ItemPointer tid, Datum *values, bool * isnull,
				   Relation heapRel);

Buffer _col_get_insert_page(Relation rel, int *need);

/* colpage.c */
void _col_relbuf(Relation rel, Buffer buf);
Buffer _col_getbuf(Relation rel, BlockNumber blkno, int access);
void _col_checkpage(Relation rel, Buffer buf);
void _col_page_mark_full(Relation rel, Buffer buf);
void _col_add_to_page(Relation rel, Buffer buf, ItemPointer tid, Datum *values, bool * isnull, int *need);
bool _col_page_has_space(Relation index, Buffer buf, int *need);

char ** _col_page_get_data(Relation index, Page page, ColumnarPageOpaque opaque);
char ** _col_page_get_nulls(Relation index, Page page, ColumnarPageOpaque opaque);
ItemPointer _col_page_get_tupleids(Relation index, Page page, ColumnarPageOpaque opaque);

#endif   /* COLUMNAR_H */
