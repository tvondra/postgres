/*-------------------------------------------------------------------------
 *
 * scrub.h
 *	  header file for scrub helper deamon
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/scrub/scrub.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCRUB_CHECKS_H
#define SCRUB_CHECKS_H

#include "postgres.h"

#include "access/heapam.h"

typedef struct ScrubCounters
{
	/* page stats */
	uint64	pages_total;
	uint64	pages_failed;

	/* checksum stats */
	uint64	checksums_total;
	uint64	checksums_failed;

	/* header stats */
	uint64	headers_total;
	uint64	headers_failed;

	/* heap content checks */
	uint64	heap_pages_total;
	uint64	heap_pages_failed;
	uint64	heap_tuples_total;
	uint64	heap_tuples_failed;

	/* toast values */
	uint64	heap_attr_toast_external_invalid;
	uint64	heap_attr_compression_broken;
	uint64	heap_attr_toast_bytes_total;
	uint64	heap_attr_toast_bytes_failed;
	uint64	heap_attr_toast_values_total;
	uint64	heap_attr_toast_values_failed;
	uint64	heap_attr_toast_chunks_total;
	uint64	heap_attr_toast_chunks_failed;

	/* btree content checks */
	uint64	btree_pages_total;
	uint64	btree_pages_failed;
	uint64	btree_tuples_total;
	uint64	btree_tuples_failed;

} ScrubCounters;

bool check_page_checksum(Relation rel, ForkNumber forkNum,
						 BlockNumber block);

bool check_page_header(Relation rel, ForkNumber forkNum,
					   Page page, BlockNumber block);

bool check_page_contents(Relation rel, ForkNumber forkNum,
						 Page page, BlockNumber block,
						 ScrubCounters *counters);

#endif /* SCRUB_CHECKS_H */
