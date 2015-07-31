/*-------------------------------------------------------------------------
 *
 * nbtinsert.c
 *	  Item insertion in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtinsert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/columnar.h"
#include "access/transam.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/tqual.h"

/*
 *	_col_doinsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routine, btinsert.
 *		By here, itup is filled in, including the TID.
 *
 *		If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
 *		will allow duplicates.  Otherwise (UNIQUE_CHECK_YES or
 *		UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
 *		For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
 *		don't actually insert.
 *
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL:
 *		it must be TRUE if the entry is known unique, else FALSE.
 *		(In the current implementation we'll also return TRUE after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool
_col_doinsert(Relation rel, ItemPointer tid, Datum *values, bool * isnull,
			  Relation heapRel)
{
	Buffer		buf;
	buf = _col_get_insert_page(rel);

	_col_add_to_page(rel, buf, tid, values, isnull);

	_col_relbuf(rel, buf);

	return true;
}
