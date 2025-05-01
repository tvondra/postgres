/*-------------------------------------------------------------------------
 *
 * relscan.h
 *	  POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/htup_details.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "nodes/tidbitmap.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/read_stream.h"
#include "storage/relfilelocator.h"
#include "storage/spin.h"
#include "utils/relcache.h"


struct ParallelTableScanDescData;

/*
 * Generic descriptor for table scans. This is the base-class for table scans,
 * which needs to be embedded in the scans of individual AMs.
 */
typedef struct TableScanDescData
{
	/* scan parameters */
	Relation	rs_rd;			/* heap relation descriptor */
	struct SnapshotData *rs_snapshot;	/* snapshot to see */
	int			rs_nkeys;		/* number of scan keys */
	struct ScanKeyData *rs_key; /* array of scan key descriptors */

	/*
	 * Scan type-specific members
	 */
	union
	{
		/* Iterator for Bitmap Table Scans */
		TBMIterator rs_tbmiterator;

		/*
		 * Range of ItemPointers for table_scan_getnextslot_tidrange() to
		 * scan.
		 */
		struct
		{
			ItemPointerData rs_mintid;
			ItemPointerData rs_maxtid;
		}			tidrange;
	}			st;

	/*
	 * Information about type and behaviour of the scan, a bitmask of members
	 * of the ScanOptions enum (see tableam.h).
	 */
	uint32		rs_flags;

	struct ParallelTableScanDescData *rs_parallel;	/* parallel scan
													 * information */
} TableScanDescData;
typedef struct TableScanDescData *TableScanDesc;

/*
 * Shared state for parallel table scan.
 *
 * Each backend participating in a parallel table scan has its own
 * TableScanDesc in backend-private memory, and those objects all contain a
 * pointer to this structure.  The information here must be sufficient to
 * properly initialize each new TableScanDesc as workers join the scan, and it
 * must act as a information what to scan for those workers.
 */
typedef struct ParallelTableScanDescData
{
	RelFileLocator phs_locator; /* physical relation to scan */
	bool		phs_syncscan;	/* report location to syncscan logic? */
	bool		phs_snapshot_any;	/* SnapshotAny, not phs_snapshot_data? */
	Size		phs_snapshot_off;	/* data for snapshot */
} ParallelTableScanDescData;
typedef struct ParallelTableScanDescData *ParallelTableScanDesc;

/*
 * Shared state for parallel table scans, for block oriented storage.
 */
typedef struct ParallelBlockTableScanDescData
{
	ParallelTableScanDescData base;

	BlockNumber phs_nblocks;	/* # blocks in relation at start of scan */
	slock_t		phs_mutex;		/* mutual exclusion for setting startblock */
	BlockNumber phs_startblock; /* starting block number */
	pg_atomic_uint64 phs_nallocated;	/* number of blocks allocated to
										 * workers so far. */
}			ParallelBlockTableScanDescData;
typedef struct ParallelBlockTableScanDescData *ParallelBlockTableScanDesc;

/*
 * Per backend state for parallel table scan, for block-oriented storage.
 */
typedef struct ParallelBlockTableScanWorkerData
{
	uint64		phsw_nallocated;	/* Current # of blocks into the scan */
	uint32		phsw_chunk_remaining;	/* # blocks left in this chunk */
	uint32		phsw_chunk_size;	/* The number of blocks to allocate in
									 * each I/O chunk for the scan */
} ParallelBlockTableScanWorkerData;
typedef struct ParallelBlockTableScanWorkerData *ParallelBlockTableScanWorker;

/*
 * Base class for fetches from a table via an index. This is the base-class
 * for such scans, which needs to be embedded in the respective struct for
 * individual AMs.
 */
typedef struct IndexFetchTableData
{
	Relation	rel;
	ReadStream *rs;
} IndexFetchTableData;

struct IndexScanInstrumentation;

/* Forward declaration, the prefetch callback needs IndexScanDescData. */
typedef struct IndexScanBatchData IndexScanBatchData;

/*
 * XXX parts of BTScanOpaqueData, BTScanPosItem and BTScanPosData relevant
 * for one batch.
 */
typedef struct IndexScanBatchPosItem	/* what we remember about each match */
{
	ItemPointerData heapTid;	/* TID of referenced heap item */
	OffsetNumber indexOffset;	/* index item's location within page */
	LocationIndex tupleOffset;	/* IndexTuple's offset in workspace, if any */
} IndexScanBatchPosItem;

/*
 * Data about one batch of items returned by the index AM. This is similar
 * to the AM-specific "opaque" structs, used by each AM to track items
 * loaded from one leaf page, but generalized for all AMs.
 *
 * XXX Not sure which of there fields are 100% needed for all index AMs,
 * most of this comes from nbtree.
 *
 * XXX Mostly a copy of BTScanPosData, but other AMs may need different (or
 * only some of those) fields.
 */
typedef struct IndexScanBatchData
{
	/*
	 * AM-specific concept of position within the index, and other stuff the
	 * AM might need to store for each batch.
	 *
	 * XXX maybe "position" is not the best name, it can have other stuff the
	 * AM needs to keep per-batch (even only for reading the leaf items, like
	 * nextTupleOffset).
	 */
	void	   *opaque;

	/*
	 * The items array is always ordered in index order (ie, increasing
	 * indexoffset).  When scanning backwards it is convenient to fill the
	 * array back-to-front, so we start at the last slot and fill downwards.
	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
	 * itemIndex is a cursor showing which entry was last returned to caller.
	 *
	 * XXX Do we need all these indexes, or would it be enough to have just
	 * 0-indexed array with only itemIndex?
	 */
	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* current index in items[] */

	/* info about killed items if any (killedItems is NULL if never used) */
	int		   *killedItems;	/* indexes of killed items */
	int			numKilled;		/* number of currently stored items */

	/*
	 * If we are doing an index-only scan, these are the tuple storage
	 * workspaces for the currPos and markPos respectively.  Each is of size
	 * BLCKSZ, so it can hold as much as a full page's worth of tuples.
	 *
	 * XXX maybe currTuples should be part of the am-specific per-batch state
	 * stored in "position" field?
	 */
	char	   *currTuples;		/* tuple storage for currPos */
	IndexScanBatchPosItem *items;	/* XXX don't size to MaxTIDsPerBTreePage */

	/*
	 * batch contents (TIDs, index tuples, kill bitmap, ...)
	 *
	 * XXX Shouldn't this be part of the "IndexScanBatchPosItem" struct? To
	 * keep everything in one place? Or why should we have separate arrays?
	 * One advantage is that we don't need to allocate memory for arrays that
	 * we don't need ... e.g. if we don't need heap tuples, we don't allocate
	 * that. We couldn't do that with everything in one struct.
	 */
	IndexTuple *itups;			/* IndexTuples, if requested */
	HeapTuple  *htups;			/* HeapTuples, if requested */
	bool	   *recheck;		/* recheck flags */

	/* XXX why do we need this on top of "opaque" pointer? */
	Datum	   *privateData;	/* private data for batch */

	/* xs_orderbyvals / xs_orderbynulls */
	Datum	   *orderbyvals;
	bool	   *orderbynulls;

} IndexScanBatchData;

/*
 * Position in the queue of batches - index of a batch, index of item in a batch.
 */
typedef struct IndexScanBatchPos
{
	int			batch;
	int			index;
} IndexScanBatchPos;

typedef struct IndexScanDescData IndexScanDescData;
typedef bool (*IndexPrefetchCallback) (IndexScanDescData * scan, void *arg, IndexScanBatchPos *pos);

/*
 * Queue
 */
typedef struct IndexScanBatches
{
	/*
	 * Did we read the last batch? The batches may be loaded from multiple
	 * places, and we need to remember when we fail to load the next batch in
	 * a given scan (which means "no more batches"). amgetbatch may restart
	 * the scan on the get call, so we need to remember it's over.
	 */
	bool		finished;
	bool		reset;

	BlockNumber lastBlock;

	/*
	 * Current scan direction, for the currently loaded batches. This is used
	 * to load data in the read stream API callback, etc.
	 *
	 * XXX May need some work to use already loaded batches after change of
	 * direction, instead of just throwing everything away. May need to reset
	 * the stream but keep the batches?
	 */
	ScanDirection direction;

	/* positions in the queue of batches (batch + item) */
	IndexScanBatchPos readPos;	/* read position */
	IndexScanBatchPos streamPos;	/* prefetch position (for read stream API) */
	IndexScanBatchPos markPos;	/* mark/restore position */

	IndexScanBatchData *markBatch;
	// IndexScanBatchData *currentBatch;

	/*
	 * Array of batches returned by the AM. The array has a capacity (but can
	 * be resized if needed). The firstBatch is an index of the first batch,
	 * but needs to be translated by (modulo maxBatches) into index in the
	 * batches array.
	 *
	 * FIXME Maybe these fields should be uint32, or something like that?
	 */
	int			maxBatches;		/* size of the batches array */
	int			firstBatch;		/* first used batch slot */
	int			nextBatch;		/* next empty batch slot */

	IndexScanBatchData **batches;

	/* callback to skip prefetching in IOS etc. */
	IndexPrefetchCallback prefetchCallback;
	void	   *prefetchArgument;
} IndexScanBatches;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData
{
	/* scan parameters */
	Relation	heapRelation;	/* heap relation descriptor, or NULL */
	Relation	indexRelation;	/* index relation descriptor */
	struct SnapshotData *xs_snapshot;	/* snapshot to see */
	int			numberOfKeys;	/* number of index qualifier conditions */
	int			numberOfOrderBys;	/* number of ordering operators */
	struct ScanKeyData *keyData;	/* array of index qualifier descriptors */
	struct ScanKeyData *orderByData;	/* array of ordering op descriptors */
	bool		xs_want_itup;	/* caller requests index tuples */
	bool		xs_temp_snap;	/* unregister snapshot at scan end? */

	/* signaling to index AM about killing index tuples */
	bool		kill_prior_tuple;	/* last-returned tuple is dead */
	bool		ignore_killed_tuples;	/* do not return killed entries */
	bool		xactStartedInRecovery;	/* prevents killing/seeing killed
										 * tuples */

	/* index access method's private state */
	void	   *opaque;			/* access-method-specific info */

	/*
	 * Instrumentation counters maintained by all index AMs during both
	 * amgettuple calls and amgetbitmap calls (unless field remains NULL)
	 */
	struct IndexScanInstrumentation *instrument;

	/*
	 * In an index-only scan, a successful amgettuple call must fill either
	 * xs_itup (and xs_itupdesc) or xs_hitup (and xs_hitupdesc) to provide the
	 * data returned by the scan.  It can fill both, in which case the heap
	 * format will be used.
	 */
	IndexTuple	xs_itup;		/* index tuple returned by AM */
	struct TupleDescData *xs_itupdesc;	/* rowtype descriptor of xs_itup */
	HeapTuple	xs_hitup;		/* index data returned by AM, as HeapTuple */
	struct TupleDescData *xs_hitupdesc; /* rowtype descriptor of xs_hitup */

	ItemPointerData xs_heaptid; /* result */
	bool		xs_heap_continue;	/* T if must keep walking, potential
									 * further results */
	IndexFetchTableData *xs_heapfetch;

	bool		xs_recheck;		/* T means scan keys must be rechecked */

	/*
	 * Batches index scan keep a list of batches loaded from the index in a
	 * circular buffer.
	 */
	IndexScanBatches *xs_batches;

	/*
	 * When fetching with an ordering operator, the values of the ORDER BY
	 * expressions of the last returned tuple, according to the index.  If
	 * xs_recheckorderby is true, these need to be rechecked just like the
	 * scan keys, and the values returned here are a lower-bound on the actual
	 * values.
	 */
	Datum	   *xs_orderbyvals;
	bool	   *xs_orderbynulls;
	bool		xs_recheckorderby;

	/* parallel index scan information, in shared memory */
	struct ParallelIndexScanDescData *parallel_scan;
}			IndexScanDescData;

/* Generic structure for parallel scans */
typedef struct ParallelIndexScanDescData
{
	RelFileLocator ps_locator;	/* physical table relation to scan */
	RelFileLocator ps_indexlocator; /* physical index relation to scan */
	Size		ps_offset_ins;	/* Offset to SharedIndexScanInstrumentation */
	Size		ps_offset_am;	/* Offset to am-specific structure */
	char		ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
}			ParallelIndexScanDescData;

struct TupleTableSlot;

/* Struct for storage-or-index scans of system tables */
typedef struct SysScanDescData
{
	Relation	heap_rel;		/* catalog being scanned */
	Relation	irel;			/* NULL if doing heap scan */
	struct TableScanDescData *scan; /* only valid in storage-scan case */
	struct IndexScanDescData *iscan;	/* only valid in index-scan case */
	struct SnapshotData *snapshot;	/* snapshot to unregister at end of scan */
	struct TupleTableSlot *slot;
}			SysScanDescData;

#endif							/* RELSCAN_H */
