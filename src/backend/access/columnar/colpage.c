/*-------------------------------------------------------------------------
 *
 * nbtpage.c
 *	  BTree-specific page management code for the Postgres btree access
 *	  method.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtpage.c
 *
 *	NOTES
 *	   Postgres btree pages look like ordinary relation pages.  The opaque
 *	   data at high addresses includes pointers to left and right siblings
 *	   and flag data describing page state.  The first page in a btree, page
 *	   zero, is special -- it stores meta-information describing the tree.
 *	   Pages one and higher store the actual tree data.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/columnar.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/snapmgr.h"
#include "common/pg_lzcompress.h"

static void _col_page_reorganize(Relation index, Buffer buf);
static void _col_page_recompress(Relation index, Buffer buf);

/*
 * When working with TIDs, we treat them as integers, and we do convert
 * them to reclaim the bits in the middle (we don't use all 16 bits for
 * the offset).
 *
 * These two macros do the trick - converting TID into int64 (although
 * the result is guaranteed to fit into 48 bits) and back.
 */
#define	TupleIdInt64(tid) \
	(ItemPointerGetBlockNumber(tid) * (int64)MaxHeapTuplesPerPage  + \
	 ItemPointerGetOffsetNumber(tid) - 1)

#define Int64TupleId(tid, x) \
	do { \
		ItemPointerSetBlockNumber(tid, x / MaxHeapTuplesPerPage); \
		ItemPointerSetOffsetNumber(tid, x % MaxHeapTuplesPerPage + 1); \
	} while (0);

/*
 * The PageSegmentData structure is defined in access/columnar.h, but
 * let's keep the macros here for now.
 */

/* has the segment NULL bitmap? (may be disabled as storage optimization) */
#define SEGMENT_HAS_NULLS(buff) \
	(!(buff->seg_flags & SEG_NULLS_DISABLED))

/* total length of the segment (including header) */
#define SEGMENT_LENGTH(seg) ((seg)->seg_len)

/* segment data length (i.e. the data buffer, including NULL bitmap) */
#define SEGMENT_DATA_LENGTH(seg) \
	(SEGMENT_LENGTH(seg) - offsetof(PageSegmentData,data))

/* segment NULL bitmap length (0 if NULLS disabled) */
#define SEGMENT_NULLS_LENGTH(seg) \
	(SEGMENT_HAS_NULLS(seg) ? (((seg)->seg_nitems + 7)/8) : 0)

/* minimum space required for the segment (header + data + NULL bitmap) */
#define SEGMENT_REQUIRED_SPACE(seg)	\
	(offsetof(PageSegmentData, data) + \
	 (seg)->seg_used_len + \
	 SEGMENT_NULLS_LENGTH(seg))

/* same as SEGMENT_REQUIRED_SPACE, but without the header */
#define SEGMENT_USED_SPACE(seg) \
	(seg->seg_used_len + SEGMENT_NULLS_LENGTH(seg))

/* check that the segment does not overflow somehow (and other checks) */
#define SEGMENT_IS_VALID(seg)	\
	(SEGMENT_REQUIRED_SPACE(seg) <= SEGMENT_LENGTH(seg))

/* amount of free space in the data buffer (between data and NULL bitmap) */
#define SEGMENT_FREE_SPACE(seg) \
	(SEGMENT_LENGTH(seg) - SEGMENT_REQUIRED_SPACE(seg))

#define SEGMENT_IS_COMPRESSED(seg) \
	((seg)->seg_used_len == (seg)->seg_comp_len)

#define SEGMENT_IS_COMPRESSIBLE(seg) \
	(!((seg)->seg_flags & SEG_COMPRESS_DISABLED))

/* index of the first NULL bitmap byte
 *
 * FIXME The (-1) seems wrong here.
 */
#define SEGMENT_NULLS_START_IDX(seg) \
	(SEGMENT_DATA_LENGTH(seg) - 1 - SEGMENT_NULLS_LENGTH(seg))

/* pointer to the first byte of the NULL bitmap */
#define SEGMENT_NULLS_START(seg) \
	(&(seg->data[SEGMENT_NULLS_START_IDX(seg)]))


static Size page_segment_size(int nitems, int typlen, bool notnull);
static bool page_segment_has_space(PageSegment buff);

static void page_segment_add_value(PageSegment buff, Datum value, bool isnull);

#define		COMPRESS_MIN_RATIO	0.75

#define		COMPRESS_RLE		1
#define		COMPRESS_DICT		2
#define		COMPRESS_PGLZ		3

static int choose_compression_method(char *data, int itemlen, int nitems);

static int recompress_pglz(PageSegment seg, ColumnarPageOpaque opaque);
static int recompress_dict(PageSegment seg, ColumnarPageOpaque opaque);
static int recompress_rle(PageSegment seg,  ColumnarPageOpaque opaque);
static int recompress_tids(PageSegment seg,  ColumnarPageOpaque opaque);

static int decompress_tids(int64 *tupleids, char *src, int srclen, int rawlen);

static int decompress_pglz(PageSegment seg, char *dest, int destlen);
static int decompress_dict(PageSegment seg, char *dest, int destlen);
static int decompress_rle(PageSegment seg,  char *dest, int destlen);

/*
 *	_col_initdatapage() -- Fill a page buffer with a correct data image
 */

void
_col_initdatapage(Relation index, Page page)
{
	int					i;
	ColumnarPageOpaque	dataopaque;
	int					freespace;	/* pd_upper - pd_lower */
	int					nitems;		/* estimated number of items */
	char			   *ptr;
	PageSegment			seg;

	TupleDesc			tupdesc = RelationGetDescr(index);

	PageInit(page, BLCKSZ, sizeof(ColumnarPageOpaqueData));

	dataopaque = (ColumnarPageOpaque) PageGetSpecialPointer(page);

	/* not really needed (zeroed by PageInit) */
	dataopaque->colpo_nitems = 0;

	/*
	 * Set pd_lower just past the end of the metadata.  This is not essential
	 * but it makes the page look compressible to xlog.c.
	 */
	((PageHeader) page)->pd_lower = PageGetContents(page) - (char*)page;

	/* free space on the page (in bytes) */
	freespace = (((PageHeader) page)->pd_upper - ((PageHeader) page)->pd_lower);

	/*
	 * Size the buffers for TIDs and column data somehow.
	 *
	 * Increment the number of items until we fill the page with
	 * uncompressed data (we'll repeat this after recompressing the
	 * data, if enabled).
	 *
	 * TODO This seems a bit wasteful - we're incrementing the number
	 *      of items by 1, so we may do hundreds of loops on every new
	 *      page. It's quite cheap (but what about 64kB pages?), but
	 *      surely we can compute some rough estimate first, or perhaps
	 *      do larger steps?
	 */
	nitems = 1;
	while (true)
	{
		/* the first buffer is always TIDs (so no NULLs) */
		Size size = page_segment_size(nitems, sizeof(int64), false);

		for (i = 0; i < tupdesc->natts; i++)
			size += page_segment_size(nitems,
									  tupdesc->attrs[i]->attlen,
									  tupdesc->attrs[i]->attnotnull);

		if (size > freespace)
		{
			/* we have to do step back */
			nitems--;
			break;
		}

		/* cool, maybe we can fit more items on the page */
		nitems++;
	}

	Assert(nitems > 0);

	/*
	 * So we know the number of items for a single page, so let's create
	 * the segments on the page, and allocate them accordingly. The first
	 * pointer is always TIDs
	 */
	ptr = PageGetContents(page);
	seg = (PageSegment)ptr;

	seg->seg_len = page_segment_size(nitems, sizeof(int64), false);

	seg->seg_used_len = 0;
	seg->seg_comp_len = 0;
	seg->seg_typ_len = sizeof(int64);
	seg->seg_nitems = 0;
	seg->seg_flags = (SEG_NULLS_DISABLED | SEG_ITEM_POINTERS | SEG_COMPRESS_DELTA_RLE);

	/* skip to the beginning of the next segment */
	ptr += SEGMENT_LENGTH(seg);

	for (i = 0; i < tupdesc->natts; i++)
	{
		seg = (PageSegment)ptr;

		seg->seg_len
			= page_segment_size(nitems,
								tupdesc->attrs[i]->attlen,
								tupdesc->attrs[i]->attnotnull);

		seg->seg_used_len = 0;
		seg->seg_comp_len = 0;
		seg->seg_typ_len = tupdesc->attrs[i]->attlen;
		seg->seg_nitems = 0;
		seg->seg_flags = (SEG_DATA | SEG_COMPRESS_AUTO);

		/*
		 * FIXME this seems not to work (indexes apparently don't inherit
		 *       attnotnull from the parent relation
		 */
		if (tupdesc->attrs[i]->attnotnull)
			seg->seg_flags |= SEG_NULLS_DISABLED;

		/* proceed to the next segment (if any) */
		ptr += SEGMENT_LENGTH(seg);
	}

	/* check we haven't overflowin into special part */
	Assert(ptr - PageGetContents(page) <= freespace);
	Assert(ptr <= PageGetSpecialPointer(page));

	/* set the page pointers so that the whole page seems full */
	((PageHeader) page)->pd_upper = ((PageHeader) page)->pd_lower;
}

/*
 * Insert page is always the last page in the relation - this checks the
 * page has space for at least one more entry (simple check as we know
 * the size of attbyval columns - will be a bit more complicated for
 * varlena attributes etc.).
 *
 * If the page does not have enough space, we try to recompress all the
 * segments, and reorganize them (shuffle them around, as the compression
 * may change space needed for each column).
 *
 * If this does not help, we allocate a completely new data page (the
 * indexes are limited to 32 columns, and with attbyval columns we're
 * guaranteed to fit the data onto the page).
 */
Buffer
_col_get_insert_page(Relation rel)
{
	/* data page for insert */
	Buffer		buf;
	Page		page;
	ColumnarPageOpaque	opaque;

	BlockNumber blocknum = InvalidBlockNumber;
	BlockNumber nblocks;

	/* FIXME probably should lower this in the future if possible */
	LockRelationForExtension(rel, ExclusiveLock);

	/* get the number of blocks in the relation */
	nblocks = RelationGetNumberOfBlocks(rel);

	/* if there are blocks in the relation, we'll look at the last block if
	 * there's some free space in it */
	if (nblocks > 0)
	{
		/* get the last block and see if it's marked as COLP_FULL */
		blocknum = (nblocks-1);

		Assert(blocknum != InvalidBlockNumber);

		buf = _col_getbuf(rel, blocknum, COL_WRITE);
		page = BufferGetPage(buf);
		opaque = (ColumnarPageOpaque) PageGetSpecialPointer(page);

		/* if it's not marked as full, see if there's enough free space */
		if (! (opaque->colpo_flags & COLP_FULL))
		{
			if (! _col_page_has_space(rel, buf))
			{
				/* try to (re)compress the page */
				_col_page_recompress(rel, buf);

				/*
				 * If the page was not marked as full, try to reorganize it by
				 * shuffling the data/null around - maybe we haven't placed those
				 * parts well initially (the varlena types and compression may
				 * change this);
				 */
				_col_page_reorganize(rel, buf);

				/* still not enough space, so mark it as full and then allocate
				 * a completely new page */
				if (! _col_page_has_space(rel, buf))
				{
					_col_page_mark_full(rel, buf);
					blocknum = InvalidBlockNumber;
				}
			}
		}
	}

	/* if we still have no block with free space in it, so allocate a new one */
	if (blocknum == InvalidBlockNumber)
	{
		buf = _col_getbuf(rel, P_NEW, COL_WRITE);
		blocknum = BufferGetBlockNumber(buf);
		page = BufferGetPage(buf);

		_col_initdatapage(rel, page);

		/* NO ELOG(ERROR) till meta is updated */
		START_CRIT_SECTION();

		MarkBufferDirty(buf);

		/* FIXME do WAL logging here */

		END_CRIT_SECTION();
	}

	/* unlock the relation (we still have lock on the buffer) */
	UnlockRelationForExtension(rel, ExclusiveLock);

	/*
	 * By here, we have a pin and read lock on the root page, and no lock set
	 * on the metadata page.  Return the root page's buffer.
	 */
	return buf;
}

void
_col_relbuf(Relation rel, Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

/*
 *	_col_getbuf() -- Get a buffer by block number for read or write.
 *
 *		blkno == P_NEW means to get an unallocated index page.  The page
 *		will be initialized before returning it.
 *
 *		When this routine returns, the appropriate lock is set on the
 *		requested buffer and its reference count has been incremented
 *		(ie, the buffer is "locked and pinned").  Also, we apply
 *		_bt_checkpage to sanity-check the page (except in P_NEW case).
 */
Buffer
_col_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer		buf;

	if (blkno != P_NEW)
	{
		/* Read an existing block of the relation */
		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, access);
		_col_checkpage(rel, buf);
	}
	else
	{
		bool	needLock = !RELATION_IS_LOCAL(rel);
		Page	page;

		if (needLock)
			LockRelationForExtension(rel, ExclusiveLock);

		buf = ReadBuffer(rel, P_NEW);

		/* Acquire buffer lock on new page */
		LockBuffer(buf, COL_WRITE);

		/*
		 * Release the file-extension lock; it's now OK for someone else to
		 * extend the relation some more.  Note that we cannot release this
		 * lock before we have buffer lock on the new page, or we risk a race
		 * condition against btvacuumscan --- see comments therein.
		 */
		if (needLock)
			UnlockRelationForExtension(rel, ExclusiveLock);

		/* Initialize the new page before returning it */
		page = BufferGetPage(buf);
		Assert(PageIsNew(page));

		/* FIXME - passing special size 0 is wrong */
		PageInit(page, BufferGetPageSize(buf), 0);
	}

	/* ref count and lock type are correct */
	return buf;
}


/*
 *	_col_checkpage() -- Verify that a freshly-read page looks sane.
 */
void
_col_checkpage(Relation rel, Buffer buf)
{
	Page		page = BufferGetPage(buf);

	/*
	 * ReadBuffer verifies that every newly-read page passes
	 * PageHeaderIsValid, which means it either contains a reasonably sane
	 * page header or is all-zero.  We have to defend against the all-zero
	 * case, however.
	 */
	if (PageIsNew(page))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("index \"%s\" contains unexpected zero page at block %u",
					RelationGetRelationName(rel),
					BufferGetBlockNumber(buf)),
				 errhint("Please REINDEX it.")));

	/*
	 * Additionally check that the special area looks sane.
	 */
	if ((PageGetSpecialSize(page) != MAXALIGN(sizeof(ColumnarPageOpaqueData))))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" contains corrupted data page at block %u",
						RelationGetRelationName(rel),
						BufferGetBlockNumber(buf)),
				 errhint("Please REINDEX it.")));
}

void
_col_page_mark_full(Relation rel, Buffer buf)
{
	Page				page = BufferGetPage(buf);
	ColumnarPageOpaque	opaque = (ColumnarPageOpaque) PageGetSpecialPointer(page);

	elog(DEBUG1, "page %d is full (%d rows)",
				 BufferGetBlockNumber(buf),
				 opaque->colpo_nitems);

	START_CRIT_SECTION();

	opaque->colpo_flags |= COLP_FULL;

	MarkBufferDirty(buf);

	/* FIXME do WAL logging here */

	END_CRIT_SECTION();

	_col_relbuf(rel, buf);
}

/*
 * Try to recompress segments on the page (combining the compressed
 * and uncompressed portions of data).
 */
static void
_col_page_recompress(Relation index, Buffer buf)
{
	int					i;
	Page				page = BufferGetPage(buf);
	char			   *ptr = (char*)PageGetContents(page);
	PageSegment			seg = (PageSegment)ptr;

	TupleDesc			tdesc = RelationGetDescr(index);
	int					natts = tdesc->natts;

	/* is there at least one compressible segment? */
	bool				has_compressible_segments = false;
	Size				saved_bytes = 0;

	ColumnarPageOpaque	opaque = (ColumnarPageOpaque) PageGetSpecialPointer(page);

	/*
	 * If recompression disabled, bail out.
	 *
	 * This can only happen if we already filled the page, tried to
	 * recompress it and found that none of the segments is compressible,
	 * but that we can shuffle the segments a bit. That can only happen
	 * if we store varlena data (so we can't accurately position the
	 * segments first) or if we're able to do some optimizations (e.g.
	 * speculatively disable NULL bitmap for some columns).
	 *
	 * So it's not highly effective, but it's also a very cheap check.
	 */
	if (opaque->colpo_flags & COLP_NO_RECOMPRESS)
		return;

	/*
	 * Recompress tuple IDs, but only if we have uncompressed data and
	 * the recompression was not previously disabled for this segment.
	 */
	if ((!SEGMENT_IS_COMPRESSED(seg)) && SEGMENT_IS_COMPRESSIBLE(seg))
		saved_bytes += recompress_tids(seg, opaque);

	/*
	 * Check whether the segment is still compressible (true unless
	 * we've marked it as incompressible now or sometimes in the past).
	 *
	 * XXX We do this check outside the recompression block in case we
	 *     ever implement incremental compression right when adding the
	 *     values (so we would get seg_used_len==seg_comp_len here).
	 */
	has_compressible_segments |= SEGMENT_IS_COMPRESSIBLE(seg);

	/* proceed to the next segment */
	ptr += SEGMENT_LENGTH(seg);

	/* must not overflow into special part */
	Assert(ptr <= PageGetSpecialPointer(page));

	for (i = 0; i < natts; i++)
	{
		int		cmethod;

		/* proceed to the next segment (so we may easily bail out) */
		seg = (PageSegment)ptr;
		ptr += SEGMENT_LENGTH(seg);

		Assert(ptr <= PageGetSpecialPointer(page));

		/*
		 * bail out if there are no plain data, or if the recompression
		 * was already disabled for this segment.
		 *
		 * XXX This is effectively the same check we've done for the TID
		 *     segment, but this time we bail using 'continue' so it's
		 *     negated. Also we do update the flag while doing that.
		 */
		if (SEGMENT_IS_COMPRESSED(seg) || (!SEGMENT_IS_COMPRESSIBLE(seg)))
		{
			has_compressible_segments |= SEGMENT_IS_COMPRESSIBLE(seg);
			continue;	/* try next segment */
		}

		/*
		 * if this is the first time we compress this page, detect the
		 * compression method, otherwise use the previously selected one
		 *
		 * FIXME we shouldn't really cram the compression type into flags,
		 *       but instead use a dedicated field for that (now we're
		 *       limited to 32 bits shared with other flags)
		 */
		if (seg->seg_flags & SEG_COMPRESS_AUTO)
			cmethod = choose_compression_method(seg->data,
												seg->seg_typ_len,
												seg->seg_nitems);
		else if (seg->seg_flags & SEG_COMPRESS_RLE)
			cmethod = COMPRESS_RLE;
		else if (seg->seg_flags & SEG_COMPRESS_DICT)
			cmethod = COMPRESS_DICT;
		else
			cmethod = COMPRESS_PGLZ;

		/* disable compression detection for this segment */
		seg->seg_flags &= (~SEG_COMPRESS_AUTO);

		switch (cmethod)
		{
			case COMPRESS_RLE:
				seg->seg_flags |= SEG_COMPRESS_RLE;
				saved_bytes += recompress_rle(seg, opaque);
				break;

			case COMPRESS_DICT:
				seg->seg_flags |= SEG_COMPRESS_DICT;
				saved_bytes += recompress_dict(seg, opaque);
				break;

			case COMPRESS_PGLZ:
				seg->seg_flags |= SEG_COMPRESS_PGLZ;
				saved_bytes += recompress_pglz(seg, opaque);
				break;
		}

		/* update the flag (true unless just marked as incompressible) */
		has_compressible_segments |= SEGMENT_IS_COMPRESSIBLE(seg);

		/* make sure it's marked correctly */
		Assert(! (seg->seg_flags & SEG_COMPRESS_AUTO));
	}

	/*
	 * If there are no compressible segments, or if we saved too little
	 * space on this recompression (so we're very unlikely to save more
	 * on the next attempt), disable further recompression of this page.
	 *
	 * We might also disable re-compression if the compressed data
	 * reached some threshold (e.g. 90% of page) to prevent frequent
	 * recompressions at the very end. OTOH there are compression
	 * methods that work efficiently in incremental mode (like RLE,
	 * which can peek at the last group and either increment the number
	 * of repetitions or create a new group).
	 */
	if ((! has_compressible_segments) || (saved_bytes < 128))
		opaque->colpo_flags |= COLP_NO_RECOMPRESS;

	elog(DEBUG1, "page %d recompressed, saved %ld bytes",
				 BufferGetBlockNumber(buf), saved_bytes);

	MarkBufferDirty(buf);
}

/*
 * Try to reorganize segments on the page, accordingly to how much space
 * they need per item. Initially the segments are sized according to
 * uncompressed size, but some columns may be more compressible than
 * others, which changes the space requirements.
 *
 * The redistribution algorithm is quite simple - assign the minimum
 * required space to each segment (so that it can hold the current data),
 * and then distribute the remaining (still free) space proportionally
 * to the already used space. So if segment A uses 2x the space compared
 * to segment B, it'll get 2x the space.
 *
 * We try to prevent frequent redistributions with only minor changes
 * i.e. if there's not free space for at least one additional row  or if
 * the redistribution would move less than 128B, we bail out immediately.
 * We don't mark the page as full though - that's not our responsibility.
 */
static void
_col_page_reorganize(Relation index, Buffer buf)
{
	int			i;
	TupleDesc	tdesc = RelationGetDescr(index);
	int			natts = tdesc->natts;

	Page		page = BufferGetPage(buf);
	char	   *ptr = PageGetContents(page);
	PageSegment	seg = (PageSegment)ptr;
	ColumnarPageOpaque	opaque = (ColumnarPageOpaque) PageGetSpecialPointer(page);

	int64	rowlen = 0,
			freespace = 0,
			usedspace = 0,
			pagespace = 0,
			redistributed = 0;

	int64  *newsizes;
	int64	totalsize;
	char  **newsegments;
	char   *freeptr;

	/*
	 * check if we've already disabled redistribution of the segments
	 *
	 * XXX This can happen if all the columns are equally compressible
	 *     (unlikely, but possible).
	 */
	if (opaque->colpo_flags & COLP_NO_REDISTRIBUTE)
		return;

	newsizes = (int64*)palloc0(sizeof(int64) * (natts+1));

	/* we can process all segments (including TupleID exactly the same) */
	for (i = 0; i <= natts; i++)
	{
		seg = (PageSegment)ptr;
		ptr += SEGMENT_LENGTH(seg);

		/* track the minimum segment size and total used space */
		newsizes[i] = MAXALIGN(SEGMENT_REQUIRED_SPACE(seg));
		usedspace  += newsizes[i];

		/* assume 1B for NULL bitmaps for each column, and alignment */
		rowlen += MAXALIGN(seg->seg_typ_len + (SEGMENT_HAS_NULLS(seg) ? 1 : 0));
	}

	/* total space available on the page ... */
	pagespace = (char*)PageGetSpecialPointer(page) - (char*)PageGetContents(page);

	/* bail out if we're unlikely to get space for at least one row */
	if ((pagespace - usedspace) < rowlen)
		return;

	/* track actually free space that we can redistribute to the segments */
	freespace = pagespace - usedspace;

	/*
	 * we don't want the headers to influence the redistribution, so we
	 * won't count them from now on for the weighting (there's one
	 * segment for each column, and one for TIDs)
	 */
	usedspace -= offsetof(PageSegmentData, data) * (natts + 1);

	Assert(usedspace > 0);

	/* now compute the new sizes for all segments, start with the first segment */
	ptr = PageGetContents(page);

	totalsize = 0;
	for (i = 0; i <= natts; i++)
	{
		int64	used, diff;

		seg = (PageSegment)ptr;
		ptr += SEGMENT_LENGTH(seg);

		/*
		 * we use SEGMENT_USED_SPACE (instead of SEGMENT_REQUIRED_SPACE) here
		 * because we don't want to include the header, to get a more accurate
		 * 'per value' space estimate
		 */
		used = SEGMENT_USED_SPACE(seg);
		diff = MAXALIGN_DOWN((freespace * used) / usedspace);

		newsizes[i] += diff;

		diff = (newsizes[i] - SEGMENT_LENGTH(seg));
		redistributed += (diff < 0) ? (-diff) : diff;

		totalsize += newsizes[i];
	}

	/* give up if we'd change less than 128B (and disable further attempts) */
	if (redistributed < 128)
	{
		opaque->colpo_flags |= COLP_NO_REDISTRIBUTE;
		pfree(newsizes);
		return;
	}

	/* Build the new segments - we can't copy them in-place right now,
	 * because we might overwrite the existing data. It's probably
	 * possible to do that somehow, but it's not worth the complexity.
	 *
	 * We'll allocate them in one chunk (including the pointer array)
	 * to minimize palloc/pfree overhead.
	 *
	 * XXX Notice all the pieces are well aligned.
	 */
	freeptr = palloc0(sizeof(char*) * (natts+1) + totalsize);

	newsegments = (char**)freeptr;
	freeptr += sizeof(char*) * (natts+1);

	ptr = PageGetContents(page);

	for (i = 0; i <= natts; i++)
	{
		PageSegment	newseg;	/* new segment pointer */

		seg = (PageSegment)ptr;
		ptr += SEGMENT_LENGTH(seg);

		newsegments[i] = freeptr;
		freeptr += newsizes[i];

		newseg = (PageSegment)newsegments[i];

		/* copy the header and the data */
		memcpy(newseg, seg, offsetof(PageSegmentData, data) + seg->seg_used_len);

		/* but modify the segment length */
		newseg->seg_len = newsizes[i];

		/* finally handle the NULL bitmap - copy it to the right place */
		if (SEGMENT_HAS_NULLS(seg))
			memcpy(SEGMENT_NULLS_START(newseg),
				   SEGMENT_NULLS_START(seg),
				   SEGMENT_NULLS_LENGTH(seg));
	}

	/* and now copy the new segments in-place */
	memset(PageGetContents(page), 0, pagespace);

	ptr = PageGetContents(page);

	for (i = 0; i <= natts; i++)
	{
		memcpy(ptr, newsegments[i], newsizes[i]);
		ptr += newsizes[i];
	}

	pfree(newsizes);
	pfree(newsegments);

	return;
}

/*
 * Add data (TIDs, datums and NULL bits) to the segments on the page.
 */
void
_col_add_to_page(Relation rel, Buffer buf, ItemPointer tid,
				 Datum *values, bool * isnull)
{
	int			i;
	Page		page = BufferGetPage(buf);
	char	   *ptr = PageGetContents(page);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	ColumnarPageOpaque opaque
		= (ColumnarPageOpaque) PageGetSpecialPointer(page);

	/* the first buffer is always TIDs */
	PageSegment		seg = (PageSegment)ptr;

	/* first add the TID to the TID buffer */
	Assert(page_segment_has_space(seg));
	page_segment_add_value(seg, Int64GetDatum(TupleIdInt64(tid)), false);

	/* go to next segment */
	ptr += SEGMENT_LENGTH(seg);

	Assert(ptr <= PageGetSpecialPointer(page));

	/* process the attribute buffers */
	for (i = 0; i < tupdesc->natts; i++)
	{
		seg = (PageSegment)ptr;

		/* only pass-by-val types at the moment */
		Assert(tupdesc->attrs[i]->attbyval);

		/* we should not get full pages here */
		Assert(page_segment_has_space(seg));

		page_segment_add_value(seg, values[i], isnull[i]);

		/* go to next segment */
		ptr += SEGMENT_LENGTH(seg);

		Assert(ptr <= PageGetSpecialPointer(page));
	}

	/* Finally increment the number of items on the page. */
	opaque->colpo_nitems += 1;

	MarkBufferDirty(buf);
}

/*
 * has space for another item with data length 'len'? (i.e. space in
 * each segment)
 */
bool
_col_page_has_space(Relation index, Buffer buf)
{
	int			i;
	Page		page = BufferGetPage(buf);
	char	   *ptr = (char*)PageGetContents(page);
	PageSegment	seg = (PageSegment)ptr;
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			natts = tupdesc->natts;

	/* TIDs segment */
	if (! page_segment_has_space(seg))
		return false;

	/* data segments */
	for (i = 0; i < natts; i++)
	{
		ptr += SEGMENT_LENGTH(seg);
		Assert(ptr <= PageGetSpecialPointer(page));

		seg = (PageSegment)ptr;

		if (! page_segment_has_space(seg))
			return false;
	}

	return true;
}

/* compression methods */

/*
 * Compression of TID array works in two steps (the TIDs are stored as
 * INT64 - see TupleIdInt64 for details).
 * 
 * 1) we compute deltas of the TIDs (assuming the TIDs are not that
 *    far from each other), keeping the first value as is
 *
 * 2) we store the TID using as few bytes as possible
 * 
 * The current implementation is quite trivial, and there's a lot to
 * gain by improving it - TID segment is usually the largest on the
 * page, so even small improvements may have significant impact on the
 * index size.
 *
 * For example, we only ever use 4 bits of the uint8 field of tid_group.
 * If we could reclaim this somehow, that'd be nice.
 */

/* represents a single TID entry, encoded into 'len' bytes */
typedef struct tid_group
{
	uint8	len:7;
	uint8	sign:1;
	char	data[1];
} tid_group;

static int
encode_tids_deltas(int64 *deltas, int nitems, char * dest)
{
	int		i;
	char *ptr = dest;

	for (i = 0; i < nitems; i++)
	{
		bool	negative = (deltas[i] >= 0) ? 0 : 1;
		int64	delta = (negative) ? (-deltas[i]) : deltas[i];

		if ((delta & 0x00000000000000FF) == delta) /* 1B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 1;
			memcpy(g->data, &delta, 1);

			ptr += offsetof(tid_group,data) + 1;
		}
		else if ((delta & 0x000000000000FFFF) == delta) /* 2B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 2;
			memcpy(g->data, &delta, 2);

			ptr += offsetof(tid_group,data) + 2;
		}
		else if ((delta & 0x0000000000FFFFFF) == delta) /* 3B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 3;
			memcpy(g->data, &delta, 3);

			ptr += offsetof(tid_group,data) + 3;
		}
		else if ((delta & 0x00000000FFFFFFFF) == delta) /* 4B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 4;
			memcpy(g->data, &delta, 4);

			ptr += offsetof(tid_group,data) + 4;
		}
		else if ((delta & 0x000000FFFFFFFFFF) == delta) /* 5B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 5;
			memcpy(g->data, &delta, 5);

			ptr += offsetof(tid_group,data) + 5;
		}
		else if ((delta & 0x0000FFFFFFFFFFFF) == delta) /* 6B */
		{
			tid_group * g = (tid_group*)ptr;

			g->sign = negative;
			g->len = 6;
			memcpy(g->data, &delta, 6);

			ptr += offsetof(tid_group,data) + 6;
		}
		else /* we only have 48-bit tuple IDs, so we should't get here */
			elog(WARNING, "invalid TID delta %ld", deltas[i]);
	}

	return (ptr - dest);
}

/*
 * decode the variable-length encoded TID deltas into INT64 deltas
 */
static int
decode_tids_deltas(char * src, int srclen, int64 * deltas, int deltalen)
{
	int		i;
	char   *ptr = src;

	i = 0;
	while (ptr < (src + srclen))
	{
		tid_group * g = (tid_group*)ptr;

		int64 delta = 0;

		memcpy(&delta, g->data, g->len);

		deltas[i] = (g->sign) ? (-delta) : delta;

		ptr += offsetof(tid_group, data) + g->len;
		i++;
	}

	Assert(i <= deltalen / sizeof(int64));

	return i;
}

/*
 * compute the last TID in the compressed stream
 *
 * This is used for incremental compression, when we need to compute the
 * first delta after the compressed section of data (recompress_tids).
 */
static int64
compute_last_tid(char *src, int srclen)
{
	int64	tid = 0;
	char   *ptr = src;

	while (ptr < (src + srclen))
	{
		tid_group * g = (tid_group*)ptr;

		int64 delta = 0;

		memcpy(&delta, g->data, g->len);

		tid +=  (g->sign) ? (-delta) : delta;

		ptr += offsetof(tid_group, data) + g->len;
	}

	/* we're expected to consume the source data exactly */
	Assert((ptr - src) == srclen);

	return tid;
}

/* fully decompress TIDs (into the TIDs represented as INT64 values) */
static int
decompress_tids(int64 *tupleids, char *src, int srclen, int rawlen)
{
	int		i;
	int		nitems = rawlen / sizeof(int64);

	Assert(rawlen % sizeof(int64) == 0);

	decode_tids_deltas(src, srclen, tupleids, rawlen);

	for (i = 1; i < nitems; i++)
		tupleids[i] = tupleids[i] + tupleids[i-1];

	return rawlen;
}

ItemPointer
_col_page_get_tupleids(Relation index, Page page, ColumnarPageOpaque opaque)
{
	int			i;
	char	   *ptr = PageGetContents(page);
	PageSegment	seg = (PageSegment)ptr;
	ItemPointer	tids;

	int64		   *dbuffer;		/* decompressed tuple IDs (encoded) */
	int				dsize;			/* decompressed data */

	Assert(seg->seg_raw_len % sizeof(int64) == 0);
	Assert(opaque->colpo_nitems == seg->seg_nitems);
	Assert(sizeof(int64) * seg->seg_nitems == seg->seg_raw_len);

	dbuffer = palloc0(seg->seg_raw_len);

	/*
	 * After decompressing the already compressed data, we expect
	 * to get this amount of data.
	 */
	dsize = seg->seg_raw_len - (seg->seg_used_len - seg->seg_comp_len);

	/*
	 * If there are already compressed data, decompress them first.
	 *
	 * We do expect the decompression to return exactly the raw_len
	 * minus the not yet compressed data.
	 *
	 */
	if (seg->seg_comp_len > 0)
	{
		int r = decompress_tids(dbuffer,
								seg->data,
								seg->seg_comp_len,
								dsize);

		if (r != dsize)
			elog(ERROR,
				 "TID decompression failed, expected %d bytes, got %d",
				 dsize, r);
	}

	/* copy the plain data into the buffer */
	memcpy((char*)dbuffer + dsize,
		   (char*)seg->data + seg->seg_comp_len,
		   seg->seg_used_len - seg->seg_comp_len);

	/* we have all the data decompressed, now let's decode the IDs */
	tids = palloc0(sizeof(ItemPointerData) * seg->seg_nitems);

	/*
	 * We store TIDs encoded into 6-byte ints (which is the same size as
	 * ItemPointerData, so we use sizeof that, but it's misleading)
	 */
	for (i = 0; i < seg->seg_nitems; i++)
		Int64TupleId(&tids[i], dbuffer[i]);

	pfree(dbuffer);

	return tids;
}

char **
_col_page_get_data(Relation index, Page page, ColumnarPageOpaque opaque)
{
	int			i;
	TupleDesc	tupdesc = RelationGetDescr(index);
	char	   *ptr = PageGetContents(page);
	PageSegment	seg = (PageSegment)ptr;

	int			totalsize = 0;
	char	   *freeptr;

	char **data;

	/*
	 * we skip the first buffer here, because that's used for TIDs,
	 * and those are loaded separately in _col_page_get_tupleids()
	 */
	ptr += SEGMENT_LENGTH(seg);

	for (i = 0; i < tupdesc->natts; i++)
	{
		seg = (PageSegment)ptr;
		ptr += SEGMENT_LENGTH(seg);

		totalsize += seg->seg_raw_len;
	}

	/* allocate data in one chunk */
	freeptr = palloc0(sizeof(char*) * tupdesc->natts + totalsize);

	data = (char**)freeptr;
	freeptr += sizeof(char*) * tupdesc->natts;

	/* we have buffer allocated, start actually reading data */

	ptr = PageGetContents(page);
	seg = (PageSegment)ptr;

	/* skip TIDs again */
	ptr += SEGMENT_LENGTH(seg);

	for (i = 0; i < tupdesc->natts; i++)
	{
		char		   *dbuffer;		/* decompressed data */

		seg = (PageSegment)ptr;

		/* proceed to the next segment (so we may easily bail out) */
		ptr += SEGMENT_LENGTH(seg);

		Assert(opaque->colpo_nitems == seg->seg_nitems);
		Assert(seg->seg_typ_len * seg->seg_nitems == seg->seg_raw_len);
		Assert(seg->seg_used_len <= seg->seg_raw_len);
		Assert(seg->seg_comp_len <= seg->seg_used_len);

		/*
		 * TODO Currently we're decompressing all the columns, because
		 *      the regular scan nodes we assume to read the existing
		 *      tuples from disk. But maybe we could skip the columns
		 *      we're not interested in somehow (and not decompress
		 *      them, saving CPU).
		 *
		 * TODO Another thing is that we might pospone the decompression
		 *      to a later time.
		 */

		dbuffer = freeptr;
		freeptr += seg->seg_raw_len;

		/*
		 * If there are already compressed data, decompress them first.
		 *
		 * We do expect the decompression to return exactly the raw_len
		 * minus the not yet compressed data.
		 *
		 */
		if (seg->seg_comp_len > 0)
		{
			int r = 0;

			if (seg->seg_flags & SEG_COMPRESS_DICT)
				r = decompress_dict(seg, dbuffer, seg->seg_raw_len);
			else if (seg->seg_flags & SEG_COMPRESS_RLE)
				r = decompress_rle(seg, dbuffer, seg->seg_raw_len);
			else if (seg->seg_flags & SEG_COMPRESS_PGLZ)
				r = decompress_pglz(seg, dbuffer, seg->seg_raw_len);
			else
				elog(ERROR, "unknown compression method");

			if (r != seg->seg_raw_len)
				elog(ERROR,
					 "data decompression failed, expected %d bytes, got %d",
					 seg->seg_raw_len, r);
		}
		else
		{
			/* copy the plain data into the buffer (the decompression does
			 * this automatically) */
			memcpy((char*)dbuffer,
				   (char*)seg->data,
				   seg->seg_used_len);
		}

		/* use the decompressed buffer */
		data[i] = dbuffer;
	}

	return data;
}

char **
_col_page_get_nulls(Relation index, Page page, ColumnarPageOpaque opaque)
{
	int			i, j;
	TupleDesc	tupdesc = RelationGetDescr(index);
	char	   *ptr = PageGetContents(page);
	PageSegment	seg = (PageSegment)ptr;

	char  **nulls;
	char   *freeptr;
	int		totalsize;

	/* we can easily compute how much space we need */
	totalsize = (sizeof(char*) + (opaque->colpo_nitems + 7 / 8));
	totalsize *= tupdesc->natts;

	freeptr = palloc0(totalsize);

	nulls = (char**)freeptr;
	freeptr += (sizeof(char*) * tupdesc->natts);

	/* skip the first buffer (TIDs, so no NULL bitmap anyway) */
	ptr += SEGMENT_LENGTH(seg);

	for (i = 0; i < tupdesc->natts; i++)
	{
		int lastbyte;
		seg = (PageSegment)ptr;

		/* skip has to happen before SEG_NULLS_DISABLED check */
		ptr += SEGMENT_LENGTH(seg);

		/* if this column has no NULLs, just leave it NULL */
		if (seg->seg_flags & SEG_NULLS_DISABLED)
		{
			elog(WARNING, "attribute %d has NULL disabled", i);
			continue;
		}

		nulls[i] = freeptr;
		freeptr += ((seg->seg_nitems + 7) / 8);

		/*
		 * but the NULL bitmap is stored in reverst, so we have to fix
		 * that first
		 */
		lastbyte = (SEGMENT_LENGTH(seg) - offsetof(PageSegmentData, data)) - 1;

		for (j = 0; j < (seg->seg_nitems + 7) / 8; j++)
			nulls[i][j] = seg->data[lastbyte-j];
	}

	return nulls;
}

/*
 * compute size required for a segment with nitems values, each havning
 * typlen bytes and NULL bitmap (unless notnull=true)
 *
 * XXX Apparently attnotnull is not passed from the table to the index,
 *     so we can't disable this at segment creation, but only speculatively
 *     during recompression (if there are no NULL values).
 */
static Size
page_segment_size(int nitems, int typlen, bool notnull)
{
	Size	header_bytes = offsetof(PageSegmentData, data);	/* header */
	Size	data_bytes   = (nitems * typlen);				/* data */
	Size	nulls_bytes  = notnull ? 0 : ((nitems + 7) / 8);	/* NULL bitmap */

	return MAXALIGN(header_bytes + nulls_bytes + data_bytes);
}

static bool
page_segment_has_space(PageSegment seg)
{
	return ((int)SEGMENT_FREE_SPACE(seg) >= (int)seg->seg_typ_len);
}

/*
 * adds a value to the uncompressed part of the segment
 *
 * This assumes there's enough free space (otherwise the segment will
 * be corrupted, by overlapping data and NULL bitmap or such).
 *
 * XXX This might also immediately add the data to the compressed part,
 *     depending on the type of compression (for RLE that should be
 *     quite simple, similarly for DICT).
 */
static void
page_segment_add_value(PageSegment seg, Datum value, bool isnull)
{
	/* copy the value in-place and move the watermark */
	if (! isnull)
		memcpy(seg->data + seg->seg_used_len, &value, seg->seg_typ_len);

	/*
	 * We always move the watermark, even for NULL values, so that
	 * we can access the element just by indexing array.
	 *
	 * FIXME won't work with varlena types (typlen < 0)
	 */
	seg->seg_used_len += seg->seg_typ_len;
	seg->seg_raw_len += seg->seg_typ_len;

	/*
	 * If the NULLS are not disabled, we have to update the NULL bitmap
	 * too, but only when isnull=true. We have to index the byte from
	 * the end of the 'data' array.
	 */
	if (isnull && (! (seg->seg_flags & SEG_NULLS_DISABLED)))
	{
		/* index of the last byte of buff->data, i.e. (length-1) */
		int lastbyte
			= (seg->seg_len - offsetof(PageSegmentData, data)) - 1;

		int byteidx = lastbyte - seg->seg_nitems / 8;
		int bitidx  = seg->seg_nitems % 8;

		Assert(byteidx >= seg->seg_used_len);

		seg->data[byteidx] |= (0x01 << bitidx);
	}

	/* check that the whole buffer is correct */
	Assert(SEGMENT_IS_VALID(seg));

	/*
	 * we increment the number of items last, so that we can use nitems
	 * as an index for NULL bitmap etc. (0-based index) directly
	 */
	seg->seg_nitems += 1;

	/* we expect raw length to be multiple of typlen */
	Assert(seg->seg_typ_len * seg->seg_nitems == seg->seg_raw_len);
}

/* used for sorting data when computing number of distinct values */
static int
compression_compare(const void *a, const void *b, void *arg)
{
	return memcmp((char*)a, (char*)b, *(int*)arg);
}

/* selects the best compression method based on the current data
 *
 * If none of the light-weight methods (RLE, DICT) does not achieve
 * at least 25% space reduction, we fall back to PGLZ (without trying
 * it first).
 */
static int
choose_compression_method(char *data, int itemlen, int nitems)
{
	int		i;
	int		ndistinct;	/* number of distinct values (DICT) */
	int		nruns;		/* number of continuous runs (RLE) */
	int		nrepeats;	/* number of repetitions in the current run (RLE) */

	/* sizes for each compression method */
	int		size_rle,
			size_dict,
			threshold;

	char   *tmp = palloc(nitems * itemlen);

	/* count distinct values (we don't care about NULLS, stored as 0) */
	memcpy(tmp, data, nitems * itemlen);
	qsort_r(tmp, nitems, itemlen, compression_compare, &itemlen);

	ndistinct = 1;
	for (i = 1; i < nitems; i++)
		ndistinct += (memcmp(tmp + (i-1)*itemlen,
							 tmp + i*itemlen,
							 itemlen)) ? 1 : 0;

	pfree(tmp);

	/* now count the continuous runs (in the original data) */
	nruns = 0;
	nrepeats = 1; /* we count the first value as a repetition */
	for (i = 1; i <= nitems; i++)
	{
		/*
		 * if we're in the same run, just increment the repetitions,
		 * otherwise slice the run into number of necessary RLE groups
		 */
		if ((i < nitems) && (!memcmp(data + (i-1)*itemlen, data + i*itemlen, itemlen)))
			nrepeats += 1;
		else /* new group or we ran out of elements*/
		{
			/* we don't allow more than 255 repetitions in a group */
			nruns += (nrepeats + 254) / 255;
			nrepeats = 1;	/* the last value */
		}
	}

	/* for RLE, each RLE run has 1B header and then the original value */
	size_rle = nruns * (1 + itemlen);

	/*
	 * we only consider DICT if there're less than 256 distinct values
	 * (so that the index fits into 1B), and we use 2^k bit indexes
	 */
	size_dict = 0;
	if (ndistinct <= 256)
	{
		/* we start with a single bit, which is enough for 2 values */
		int	nbits = 1;
		int	nvals = 2;

		while (nvals < ndistinct)
		{
			nbits *= 2;
			nvals *= nvals;
		}

		/* for DICT we use 2B header (number of keys, number of items) */
		size_dict = 2 + (ndistinct * itemlen) + (nbits * nitems + 7) / 8;
	}

	threshold = (double)(itemlen * nitems) * COMPRESS_MIN_RATIO;

	/* check if we reached the min compression ratio */
	if (size_rle > threshold)
		size_rle = 0;

	if (size_dict > threshold)
		size_dict = 0;

	if ((size_rle > 0) && (size_dict > 0))
		return (size_rle < size_dict) ? COMPRESS_RLE : COMPRESS_DICT;
	else if (size_rle > 0)
		return COMPRESS_RLE;
	else if (size_dict > 0)
		return COMPRESS_DICT;

	return COMPRESS_PGLZ;
}

static int
recompress_pglz(PageSegment seg, ColumnarPageOpaque opaque)
{
	int		saved_bytes =0 ;	/* saved by recompression */
	char   *dbuffer;			/* decompressed data */
	char   *cbuffer;			/* compressed buffer */
	int		cbuffersize;		/* compression buffer size */
	int		dsize;				/* decompressed data */
	int		csize;				/* compressed data */

	Assert(opaque->colpo_nitems == seg->seg_nitems);
	Assert(seg->seg_typ_len * seg->seg_nitems == seg->seg_raw_len);

	dbuffer = (char*)palloc0(seg->seg_raw_len);

	/*
	 * After decompressing the already compressed data, we expect
	 * to get this amount of data.
	 */
	dsize = seg->seg_raw_len - (seg->seg_used_len - seg->seg_comp_len);

	/*
	 * If there are already compressed data, decompress them first.
	 *
	 * We do expect the decompression to return exactly the raw_len
	 * minus the not yet compressed data.
	 *
	 */
	if (seg->seg_comp_len > 0)
	{
		int r = pglz_decompress(seg->data,
								seg->seg_comp_len,
								dbuffer,
								dsize);

		if (r != dsize)
			elog(ERROR,
				 "data decompression failed, expected %d bytes, got %d",
				 dsize, r);
	}

	/* copy the plain data into the buffer */
	memcpy((char*)dbuffer + dsize,
		   (char*)seg->data + seg->seg_comp_len,
		   seg->seg_used_len - seg->seg_comp_len);

	/* we have all the data decompressed, now let's compress them */

	/*
	 * the compression buffer 2x the size of raw data
	 *
	 * FIXME that's probably excessive, exact value depends on the
	 *       compression algorithm / implementation
	 */
	cbuffersize = 2*seg->seg_raw_len;
	cbuffer = palloc0(cbuffersize);

	csize = pglz_compress(dbuffer, seg->seg_raw_len, cbuffer,
						  PGLZ_strategy_always);

	/*
	 * If the compression actually reduced size, we'll use that.
	 *
	 * XXX we could also use some minimum compression ratio (e.g. 75%)
	 */
	if ((csize > 0) && (csize < seg->seg_used_len))
	{
		/* remember we found a compressible segment and how much space we saved */
		saved_bytes = seg->seg_used_len - csize;

		memcpy(seg->data, cbuffer, csize);
		seg->seg_used_len = csize;
		seg->seg_comp_len = csize;

		/*
		 * zero the part that may be reused for NULL bitmap, unless NULLs
		 * were disabled for the segment
		 */
		if (! (seg->seg_flags & SEG_NULLS_DISABLED))
			memset(seg->data + csize, 0,
				   seg->seg_len - offsetof(PageSegmentData, data)
								- csize - (seg->seg_nitems + 7) / 8);
	}
	else
		/* can't recompress, so disable further attempts */
		seg->seg_flags |= SEG_COMPRESS_DISABLED;

	pfree(dbuffer);
	pfree(cbuffer);

	return saved_bytes;
}

static int
decompress_pglz(PageSegment seg, char *dest, int destlen)
{
	int		dsize = seg->seg_raw_len - (seg->seg_used_len - seg->seg_comp_len);

	Assert(destlen >= seg->seg_raw_len);
	Assert(seg->seg_flags & SEG_COMPRESS_PGLZ);

	Assert(seg->seg_used_len >= seg->seg_comp_len);
	Assert((seg->seg_used_len - seg->seg_comp_len) % seg->seg_typ_len == 0);

	/*
	 * If there are already compressed data, decompress them first.
	 *
	 * We do expect the decompression to return exactly the raw_len
	 * minus the not yet compressed data.
	 *
	 */
	if (seg->seg_comp_len > 0)
	{
		int r = pglz_decompress(seg->data,
								seg->seg_comp_len,
								dest,
								dsize);

		if (r != dsize)
			elog(ERROR,
				 "data decompression failed, expected %d bytes, got %d",
				 dsize, r);
	}

	/* copy the plain data into the buffer */
	memcpy((char*)dest + dsize,
		   (char*)seg->data + seg->seg_comp_len,
		   seg->seg_used_len - seg->seg_comp_len);

	return seg->seg_raw_len;
}

typedef struct dict_header {
	uint8	nkeys;	/* length of the dictionary */
	uint8	nbits;	/* length of a key (2^k bits) */
	char	data[1];
} dict_header;

/* used only for qsort/bsearch only (not storage) */
typedef struct dict_element {
	uint8	len;
	char   *value;
} dict_element;

static int
dict_compare(const void *a, const void *b)
{
	dict_element *da = (dict_element*)a;
	dict_element *db = (dict_element*)b;

	Assert((da->len >= 1) && (da->len <= 8));
	Assert(da->len == da->len);

	return memcmp(da->value, db->value, da->len);
}

static void
dict_expand(uint8 *data, int nbits, int nitems, int *indexes, int *map)
{
	int		i;
	int		perbyte = (8 / nbits);

	uint8	mask = 0xFF;

	Assert(nbits >= 1);
	Assert(nbits <= 8);
	Assert(nbits * perbyte == 8);	/* completely split each byte */

	/* keep only the necessary number of bits */
	mask = (mask >> (8 - nbits));

	for (i = 0; i < nitems; i++)
	{
		int	byte   = (i / perbyte);
		int	offset = (i % perbyte);

		indexes[i] = (data[byte] & (mask << (offset * nbits))) >> (offset * nbits);

		if (map != NULL)
			indexes[i] = map[indexes[i]];
	}
}

static int
dict_nbits(int ndistinct)
{
	int nbits = 1;
	int nvals = 2;

	while (nvals < ndistinct)
	{
		nvals *= nvals;
		nbits *= 2;
	}

	Assert(nbits <= 8);
	Assert(nvals <= 256);

	return nbits;
}

static int*
dict_build_map(dict_element * elements, int nelements, char *keys, int nkeys, int keylen)
{
	int		i;
	char   *ptr = keys;

	int	   *map = (int*)palloc0(sizeof(int) * nkeys);

	/* lookup each key from the old dict in the new one, store the mapping */
	for (i = 0; i < nkeys; i++)
	{
		dict_element s;
		dict_element *e;

		s.len = keylen;
		s.value = ptr;

		e = bsearch(&s, elements, nelements, sizeof(dict_element), dict_compare);

		map[i] = (e - elements);
		ptr += keylen;	/* next key */
	}

	return map;
}

static int
recompress_dict(PageSegment seg, ColumnarPageOpaque opaque)
{
	int		i;
	int		saved_bytes = 0;
	int		ndistinct;	/* number of distinct values (DICT) */

	/* used for sorting / distinct counting */
	dict_element   *elements;
	char		   *ptr;

	/* items in the uncompressed part */
	int		itemlen = seg->seg_typ_len;
	int		nitems = (seg->seg_used_len - seg->seg_comp_len) / itemlen;

	/* count distinct values first (count NULLS as 0) */

	/* if there is already-compressed part, we need to count the keys
	 * as regular items (when counting ndistinct) */
	if (seg->seg_comp_len > 0)
	{
		int i, j = 0;
		dict_header *dict = (dict_header*)seg->data;

		/* from now on we keep with this size of 'elements' */
		nitems += (int)dict->nkeys;

		elements = palloc(nitems * sizeof(dict_element));

		/* first add the existing dict keys, then uncompressed data */
		ptr = seg->data + offsetof(dict_header, data); 	/* first dict key */

		for (i = 0; i < dict->nkeys; i++)
		{
			Assert(j < nitems);

			elements[j].len = itemlen;
			elements[j].value = ptr;

			j++;
			ptr += itemlen;
		}

		/* now the uncompressed data */
		ptr = seg->data + seg->seg_comp_len;	/* first uncompressed value */

		while (ptr < seg->data + seg->seg_used_len)
		{
			Assert(j < nitems);

			elements[j].len = itemlen;
			elements[j].value = ptr;

			j++;
			ptr += itemlen;
		}
	}
	else
	{
		int i, j = 0;
		elements = palloc(nitems * sizeof(dict_element));

		ptr = seg->data;

		for (i = 0; i < nitems; i++)
		{
			elements[j].len = itemlen;
			elements[j].value = ptr;

			j++;
			ptr += itemlen;
		}
	}

	/* sort the data */
	pg_qsort(elements, nitems, sizeof(dict_element), dict_compare);

	/* count the distinct values, and also build the new dict in one pass */
	ndistinct = 1;
	for (i = 1; i < nitems; i++)
	{
		if (dict_compare(&elements[i], &elements[i-1]) != 0)
		{
			if (ndistinct < i)
				memcpy(&elements[ndistinct], &elements[i], sizeof(dict_element));

			ndistinct++;
		}
	}

	if (ndistinct <= 256)	/* not too many values - (re)compress */
	{
		int		nbits;
		int		dictlen,	/* length of dictionary (header + keys) */
				datalen;	/* length of encoded indexes (into dict) */

		/* new (uncompressed) items */
		int nitems = (seg->seg_used_len - seg->seg_comp_len)/itemlen;

		/* old (compressed) items */
		int oitems = seg->seg_nitems - nitems;

		/* compressed data (dict + indexes) */
		char   *cbuffer;
		char   *ptr;

		/* pointers into the cbuffer for convenience */
		dict_header	   *dhead;	/* dictionary head */
		char		   *keys;	/* dictionary keys */
		uint8		   *data;	/* encoded indexes */

		/* indexes to the dictionary (one for each item in the segment) */
		int	   *indexes = palloc0(seg->seg_nitems * sizeof(int));

		/* derive key size (in bits) */
		nbits = dict_nbits(ndistinct);

		/* size of the dictionary (header + keys) */
		dictlen = offsetof(dict_header, data) + ndistinct * itemlen;

		/* length of encoded values (using the dictionary) */
		datalen = (seg->seg_nitems * nbits + 7) / 8;	/* in bytes */

		cbuffer = palloc0(dictlen+datalen);

		dhead = (dict_header*)cbuffer;
		keys  = dhead->data;
		data  = (uint8*)(keys + ndistinct * itemlen);

		/* store dict metadata */
		dhead->nkeys = ndistinct;
		dhead->nbits = nbits;

		/* copy the new dict into the cbuffer (just the original items) */
		ptr = keys;
		for (i = 0; i < ndistinct; i++)
		{
			Assert((ptr - keys) < itemlen * ndistinct);

			memcpy(ptr, elements[i].value, itemlen);
			ptr += itemlen;
		}

		/* we sized the buffer so that data start right after the keys */
		Assert(ptr == (char*)data);

		/* next build the indexes - first expand the already compressed
		 * data if we have some - we have to use the old dictionary */
		if (seg->seg_comp_len > 0)
		{
			/* old dictionary */
			dict_header *odict = (dict_header*)seg->data;

			/* data compressed using the old dict */
			uint8 *odata = (uint8*)seg->data + offsetof(dict_header,data)
											+ (odict->nkeys * itemlen);

			/* build map of indexes between old and new dictionaries */
			int * map = dict_build_map(elements, ndistinct,
									   odict->data, odict->nkeys, itemlen);

			/* now expand the data (and use the index map we just built) */
			dict_expand(odata, odict->nbits, oitems, indexes, map);

			pfree(map);
		}

		/* now we have to process the new uncompressed data */
		ptr = seg->data + seg->seg_comp_len;
		i = oitems;

		while (ptr < seg->data + seg->seg_used_len)
		{
			dict_element *e;	/* search result */
			dict_element s;		/* search key */

			s.len = itemlen;
			s.value = ptr;

			e = bsearch(&s,
						elements, ndistinct, sizeof(dict_element),
						dict_compare);

			Assert(e != NULL);
			Assert(e < elements + ndistinct);

			indexes[i++] = (e - elements);

			Assert(i <= seg->seg_nitems);

			ptr += itemlen;
		}

		/* and now encode the indexes using just nbits per key */
		for (i = 0; i < seg->seg_nitems; i++)
		{
			int perbyte = 8 / nbits;	/* keys per byte */

			/* make sure we're still within the array */
			Assert((i/perbyte) < datalen);

			data[i / perbyte] |= (((uint8)indexes[i]) << ((i % perbyte) * nbits));
		}

		/*
		 * if we've actually saved space compared to the previous state,
		 * store the new compressed form - this may happen very easily
		 * e.g. if we have to switch from 4bit to 8bit keys
		 *
		 * If that happens, we can't just copy the data in place because
		 * the segment may not have enough space.
		 *
		 * This is a bit annoying shortcoming, but not necessarily fatal.
		 * The pare reorganization that happens after the recompression
		 * will increase the size for this page segment. And on the next
		 * recompression, we may have enough space (but not necessarily,
		 * because the reorganization resizes the segments so that all
		 * are expected to fill at the same time).
		 *
		 * Also, this check compares the size with current storage size,
		 * but what probably matters is comparison to the source data.
		 *
		 * TODO detect this sooner and skip the compression entirely
		 */
		if ((dictlen + datalen) < seg->seg_used_len)
		{
			/* keep track of how much space we saved */
			saved_bytes = seg->seg_used_len - (dictlen + datalen);

			/* reset the existing data (so that we don't break future NULL bitmap) */
			memset(seg->data, 0, seg->seg_used_len);

			/* copy the compressed data in-place */
			memcpy(seg->data, cbuffer, dictlen + datalen);

			/* and finally update the counters */
			seg->seg_comp_len = (dictlen + datalen);
			seg->seg_used_len = (dictlen + datalen);
		}

		pfree(indexes);
		pfree(cbuffer);
	}

	if (saved_bytes == 0)
		seg->seg_flags |= SEG_COMPRESS_DISABLED;

	pfree(elements);

	return saved_bytes;
}

static int
decompress_dict(PageSegment seg, char *dest, int destlen)
{
	char   *dptr = dest;

	int citems;	/* number of items stored in the compressed part */

	Assert(destlen >= seg->seg_raw_len);
	Assert(seg->seg_flags & SEG_COMPRESS_DICT);

	Assert(seg->seg_used_len >= seg->seg_comp_len);
	Assert((seg->seg_used_len - seg->seg_comp_len) % seg->seg_typ_len == 0);

	/* compute number of items stored in the compressed part */
	citems = seg->seg_nitems - (seg->seg_used_len - seg->seg_comp_len) % seg->seg_typ_len;

	Assert(citems >= 0);

	/* if there are compressed data, find */
	if (seg->seg_comp_len > 0)
	{
		int i;

		/* dictionary */
		dict_header *dict = (dict_header*)seg->data;

		/* keys of the dictionary */
		char *keys = seg->data + offsetof(dict_header,data);

		/* data (indexes of the keys) */
		uint8 *data = (uint8*)keys + (dict->nkeys * seg->seg_typ_len);

		int *indexes = palloc0(citems * sizeof(int));

		/* check that we get the right number of bytes (matching seg_comp_len) */
		Assert(((char*)data - seg->data) + ((citems * dict->nbits + 7) / 8) == seg->seg_comp_len);

		/* expand the compressed data into indexes (into the keys) */
		dict_expand(data, dict->nbits, citems, indexes, NULL);

		for (i = 0; i < citems; i++)
		{
			Assert((indexes[i] >= 0) && (indexes[i] < dict->nkeys));

			memcpy(dptr, keys + (indexes[i] * seg->seg_typ_len), seg->seg_typ_len);
			dptr += seg->seg_typ_len;
		}

		pfree(indexes);

		/* make sure we produced exactly the amount of data */
		Assert((dptr - dest) == seg->seg_raw_len - (seg->seg_used_len - seg->seg_comp_len));
	}

	/* if there are decompressed data, copy them to the output */
	if (seg->seg_used_len > seg->seg_comp_len)
	{
		memcpy(dptr, seg->data + seg->seg_comp_len,
					(seg->seg_used_len - seg->seg_comp_len));
		dptr += (seg->seg_used_len - seg->seg_comp_len);
	}

	/* have we produced exactly the expected amount of data */
	Assert((dptr - dest) == seg->seg_raw_len);

	return (dptr - dest);
}

typedef struct rle_group {
	uint8	repeats;
	char	data[1];
} rle_group;

static int
recompress_rle(PageSegment seg, ColumnarPageOpaque opaque)
{
	int		i;
	int		saved_bytes =0 ;	/* saved by recompression */
	char   *data = seg->data + seg->seg_comp_len;
	char   *cbuffer;
	char   *ptr;
	rle_group *rle, *last;
	int		nrepeats;
	int		rlelen;
	bool	merge_last = false;

	/* items in the uncompressed part */
	int		itemlen = seg->seg_typ_len;
	int		nitems = (seg->seg_used_len - seg->seg_comp_len) / itemlen;

	/* allocate a single RLE entry for reuse */
	rlelen = offsetof(rle_group,data) + itemlen;
	rle = palloc(rlelen);

	/* get the last group in the already compressed data */
	last = (rle_group*)((seg->seg_comp_len > 0) ? (seg->data + seg->seg_comp_len - rlelen) : NULL);

	/* assume we create a separate group for each item (worst case) */
	cbuffer = palloc(nitems * (itemlen + 1));
	ptr = cbuffer;	/* next group */

	/* walk through the data and append  */
	nrepeats = 1; /* we count the first value as a repetition */
	for (i = 1; i <= nitems; i++)
	{
		/*
		 * if we're in the same run, just increment the repetitions,
		 * otherwise slice the run into number of necessary RLE groups
		 */
		if ((i < nitems) && (!memcmp(data + (i-1)*itemlen, data + i*itemlen, itemlen)))
			nrepeats += 1;
		else /* new group or we ran out of elements*/
		{
			/* need to copy the previous item */
			memcpy(rle->data, data + (i-1)*itemlen, itemlen);

			/*
			 * if this is the first group, we'll try to create a 'merge' group,
			 * but only for the first group in this run
			 *
			 * We can't merge it directly because we don't know whether the
			 * recompression will be successfull or not yet.
			 */
			if ((ptr == cbuffer) && (last != NULL) && (memcmp(last->data, rle->data, itemlen) == 0))
			{
				/* ok, we'll create merge group - let's see how much repeats we
				 * can add to the last group */
				int remaining = 255 - (int)last->repeats;

				merge_last = true;

				/* we'll need more than we can fit merge into the last group */
				rle->repeats = (remaining < nrepeats) ? remaining : nrepeats;
				memcpy(ptr, rle, rlelen);
				ptr += rlelen;

				nrepeats -= remaining;
			}

			/* we don't allow more than 255 repetitions in a group */
			while (nrepeats >= 255)
			{
				rle->repeats = 255;
				memcpy(ptr, rle, rlelen);
				ptr += rlelen;
				nrepeats -= 255;
			}

			if (nrepeats > 0)
			{
				rle->repeats = nrepeats;
				memcpy(ptr, rle, rlelen);
				ptr += rlelen;
			}

			nrepeats = 1;
		}
	}

	pfree(rle);

	/* must not exceed the end of cbuffer */
	Assert((ptr - cbuffer) < nitems * (itemlen + 1));

	/* if we've actually saved space, replace the stored data
	 *
	 * FIXME we should tweak the 'saved bytes' before this check (using
	 *       merge_group flag), not within the block (that's too late),
	 *       but the difference is likely very small
	 */
	if ((ptr - cbuffer) < (seg->seg_used_len - seg->seg_comp_len))
	{
		int	clen = (ptr - cbuffer);

		/* reset the pointer, so that we can tweak it in case of merging */
		ptr = cbuffer;

		/* if we're supposed to merge the last/first group, do that now
		 * and tweak the byte counters */
		if (merge_last)
		{
			last->repeats += ((rle_group*)ptr)->repeats;
			ptr += rlelen;
			clen -= rlelen;
		}

		/* remember how much space we saved */
		saved_bytes = (seg->seg_used_len - seg->seg_comp_len) - clen;

		/* zero the memory (so that we don't break future NULL bits) */
		memset(seg->data + seg->seg_comp_len, 0,
			   seg->seg_used_len - seg->seg_comp_len);

		/* copy the newly compressed data in-place */
		memcpy(seg->data + seg->seg_comp_len, ptr, clen);

		/* and finally update the pointers */
		seg->seg_comp_len += clen;
		seg->seg_used_len = seg->seg_comp_len;
	}
	else
		seg->seg_flags |= SEG_COMPRESS_DISABLED;

	pfree(cbuffer);

	return saved_bytes;
}

static int
decompress_rle(PageSegment seg,  char *dest, int destlen)
{
	char *dptr = dest;

	/* we expect a buffer that can hold all the data */
	Assert(destlen >= seg->seg_raw_len);

	/* if the segment is not RLE compressed, something went wrong */
	Assert(seg->seg_flags & SEG_COMPRESS_RLE);

	/* if there are compressed data, we'll decompress them first */
	if (seg->seg_comp_len > 0)
	{
		char   *ptr = seg->data;
		char   *end = (seg->data + seg->seg_comp_len);

		/* the compressed data is a whole multiple of RLE group size */
		Assert(seg->seg_comp_len % (offsetof(rle_group,data) + seg->seg_typ_len) == 0);

		while (ptr < end)
		{
			int	i;
			rle_group *g = (rle_group *)ptr;

			Assert(g->repeats > 0);

			for (i = 0; i < (int)g->repeats; i++)
			{
				memcpy(dptr, g->data, seg->seg_typ_len);
				dptr += seg->seg_typ_len;
			}

			ptr += offsetof(rle_group,data) + seg->seg_typ_len;
		}

		/* we're assumed to consume the compressed data exactly */
		Assert(ptr == end);

		/*
		 * and expected to produce the part of raw data that is not
		 * stored in the uncompressed part
		 */
		Assert((dptr - dest) == (seg->seg_raw_len - (seg->seg_used_len - seg->seg_comp_len)));
	}

	/* if there are decompressed data, copy them to the output */
	if (seg->seg_used_len > seg->seg_comp_len)
	{
		memcpy(dptr, seg->data + seg->seg_comp_len,
					(seg->seg_used_len - seg->seg_comp_len));
		dptr += (seg->seg_used_len - seg->seg_comp_len);
	}

	/* have we produced exactly the expected amount of data */
	Assert((dptr - dest) == seg->seg_raw_len);

	return (dptr - dest);
}

static int
recompress_tids(PageSegment seg, ColumnarPageOpaque opaque)
{
	int		i;
	int		saved_bytes = 0;
	int64  *tupleids;		/* pointer to the (uncompressed) TIDs */
	int		ndeltas;		/* number of uncompressed TIDs (deltas) */
	int64  *deltas;			/* deltas computed for the TIDs */
	char   *cbuffer;		/* compressed buffer */
	int		cbufflen;		/* compression buffer size (max) */
	int		clen;			/* actually compressed data */
	int64	last = 0;		/* last TID in the compressed part */

	/* basic segment-level checks */
	Assert(opaque->colpo_nitems == seg->seg_nitems);
	Assert(sizeof(int64) * seg->seg_nitems == seg->seg_raw_len);
	Assert(seg->seg_used_len > seg->seg_comp_len);

	/* the uncompressed data are actually TIDs */
	Assert((seg->seg_used_len - seg->seg_comp_len) % sizeof(int64) == 0);

	/* so how many TIDs are not yet compressed */
	ndeltas = (seg->seg_used_len - seg->seg_comp_len) / sizeof(int64);
	deltas = (int64 *)palloc0(sizeof(int64) * ndeltas);

	/* the TIDs (just a reference) */
	tupleids = (int64*)(seg->data + seg->seg_comp_len);

	/*
	 * if there are no compressed data, we can compute the deltas very
	 * easily directly from the int64 array
	 *
	 * if there already are compressed data, we have to compute the
	 * last TID (so that we can use it to compute deltas for the
	 * uncompressed part).
	 *
	 * FIXME This is quite expensive, significantly slowing down the
	 *       whole recompression - determining the last ID needs to
	 *       be faster (e.g. tracking it somehow separately).
	 */
	if (seg->seg_comp_len > 0)
		last = compute_last_tid(seg->data, seg->seg_comp_len);

	/* copy the first element in-place */
	deltas[0] = tupleids[0] - last;

	for (i = 1; i < ndeltas; i++)
		deltas[i] = tupleids[i] - tupleids[i-1];

	/* assume each delta needs the full 8B entry, plus 1B header */
	cbufflen = ndeltas * 9;
	cbuffer = palloc0(cbufflen);

	/* now just call the compression method on the deltas */
	clen = encode_tids_deltas(deltas, ndeltas, cbuffer);

	/*
	 * If the compression actually reduced size, we'll use that.
	 *
	 * Don't forget the 'clen' only tracks the newly compressed data,
	 * so only compare it to (seg_used_len - seg_comp_len), not to the
	 * whole buffer.
	 *
	 * Also, don't replace the original data, but append it.
	 *
	 * XXX we could also use some minimum compression ratio (e.g. 75%)
	 */
	if ((clen > 0) && (clen < (seg->seg_used_len - seg->seg_comp_len)))
	{
		/* remember we found a compressible segment and how much space we saved */
		saved_bytes += (seg->seg_used_len - seg->seg_comp_len) - clen;

		memcpy(seg->data + seg->seg_comp_len, cbuffer, clen);
		seg->seg_comp_len += clen;
		seg->seg_used_len = seg->seg_comp_len;

		/* TIDs have always NULLs disabled, so no zeroing needed */
	}
	else
		/* can't recompress, so disable further attempts */
		seg->seg_flags |= SEG_COMPRESS_DISABLED;

	pfree(deltas);
	pfree(cbuffer);

	return saved_bytes;
}
