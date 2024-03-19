/*
 * Copy entire files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_combinebackup/copy_file.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_FILE_H
#define COPY_FILE_H

#include "c.h"
#include "common/checksum_helper.h"
#include "common/file_utils.h"

/* XXX do we even want this? how does pg_upgrade to this? */
typedef enum CopyFileMethod
{
	PG_COPYFILE_FALLBACK = 0x1,
	PG_COPYFILE_IOCTL_FICLONE = 0x2,	/* Linux */
	PG_COPYFILE_COPY_FILE_RANGE = 0x4,	/* FreeBSD & Linux >= 4.5 */
	PG_COPYFILE_COPYFILE_CLONE_FORCE = 0x8	/* MacOS */
} CopyFileMethod;
#define PG_COPYFILE_ANY_WITH_FALLBACK (2 << 4) - 1

extern void copy_file(const char *src, const char *dst,
					  pg_checksum_context *checksum_ctx, bool dry_run,
					  CopyFileMethod copy_strategy);

#endif							/* COPY_FILE_H */
