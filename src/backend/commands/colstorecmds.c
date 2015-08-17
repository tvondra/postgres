/*-------------------------------------------------------------------------
 *
 * colstorecmds.c
 *	  column store creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/colstorecmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/colstore.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_cstore_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "colstore/colstoreapi.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * Convert a handler function name passed from the parser to an Oid.
 */
static Oid
lookup_cstore_handler_func(DefElem *handler)
{
	Oid			handlerOid;
	Oid			funcargtypes[1];

	if (handler == NULL || handler->arg == NULL)
		return InvalidOid;

	/* handlers have no arguments */
	handlerOid = LookupFuncName((List *) handler->arg, 0, funcargtypes, false);

	/* check that handler has correct return type */
	if (get_func_rettype(handlerOid) != CSTORE_HANDLEROID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return type \"cstore_handler\"",
						NameListToString((List *) handler->arg))));

	return handlerOid;
}

/*
 * Process function options of CREATE COLUMN STORE ACCESS METHOD
 */
static void
parse_func_options(List *func_options,
				   bool *handler_given, Oid *csthandler)
{
	ListCell   *cell;

	*handler_given = false;
	*csthandler = InvalidOid;

	foreach(cell, func_options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "handler") == 0)
		{
			if (*handler_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			*handler_given = true;
			*csthandler = lookup_cstore_handler_func(def);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 def->defname);
	}
}

/*
 * Create a column store access method
 */
ObjectAddress
CreateColumnStoreAM(CreateColumnStoreAMStmt *stmt)
{
	Relation	rel;
	Datum		values[Natts_pg_cstore_am];
	bool		nulls[Natts_pg_cstore_am];
	HeapTuple	tuple;
	Oid			cstamId;
	bool		handler_given;
	Oid			csthandler;
	ObjectAddress myself;
	ObjectAddress referenced;

	rel = heap_open(CStoreAmRelationId, RowExclusiveLock);

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("permission denied to create column store access method \"%s\"",
				   stmt->cstamname),
			errhint("Must be superuser to create a column store access method.")));

	/*
	 * Check that there is no other column store AM by this name.
	 */
	if (GetColumnStoreAMByName(stmt->cstamname, true) != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("column store access method \"%s\" already exists",
						stmt->cstamname)));

	/*
	 * Insert tuple into pg_foreign_data_wrapper.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_cstore_am_cstamname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->cstamname));

	/* Lookup handler and validator functions, if given */
	parse_func_options(stmt->func_options, &handler_given, &csthandler);

	if (! handler_given)
		elog(ERROR, "column store access method requires METHOD option");

	values[Anum_pg_cstore_am_cstamhandler - 1] = ObjectIdGetDatum(csthandler);

	tuple = heap_form_tuple(rel->rd_att, values, nulls);

	cstamId = simple_heap_insert(rel, tuple);
	CatalogUpdateIndexes(rel, tuple);

	heap_freetuple(tuple);

	/* record dependencies */
	myself.classId = CStoreAmRelationId;
	myself.objectId = cstamId;
	myself.objectSubId = 0;

	if (OidIsValid(csthandler))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = csthandler;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new column store access method */
	InvokeObjectPostCreateHook(CStoreAmRelationId, cstamId, 0);

	heap_close(rel, RowExclusiveLock);

	return myself;
}
