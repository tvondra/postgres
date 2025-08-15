/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/block.h"

/*
 * Checksum version 0 is used for when data checksums are disabled (OFF).
 * PG_DATA_CHECKSUM_VERSION defines that data checksums are enabled in the
 * cluster and PG_DATA_CHECKSUM_INPROGRESS_{ON|OFF}_VERSION defines that data
 * checksums are either currently being enabled or disabled.
 */
typedef enum ChecksumType
{
	PG_DATA_CHECKSUM_OFF = 0,
	PG_DATA_CHECKSUM_VERSION,
	PG_DATA_CHECKSUM_INPROGRESS_ON_VERSION,
	PG_DATA_CHECKSUM_INPROGRESS_OFF_VERSION
} ChecksumType;

/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);

#endif							/* CHECKSUM_H */
