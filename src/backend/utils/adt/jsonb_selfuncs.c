/*-------------------------------------------------------------------------
 *
 * jsonb_selfuncs.c
 *	  Functions for selectivity estimation of JSONB operators
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_selfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

/* the default selectivity matches contsel() */
#define DEFAULT_CONTAIN_SEL	0.001

static Selectivity calc_jsonbcontsel(VariableStatData *vardata, Datum constval,
									 Oid operator);
static Selectivity mcelem_jsonb_selec(Jsonb *jsonb,
				   TypeCacheEntry *typentry,
				   Datum *mcelem, int nmcelem,
				   float4 *numbers, int nnumbers,
				   Oid operator, FmgrInfo *cmpfunc);
static List *jsonb_get_paths(Jsonb *jb);
/*
 * jsonbcontsel -- restriction selectivity for jsonb @>, <@ operators
 */
Datum
jsonbcontsel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Oid			operator = PG_GETARG_OID(1);
	List	   *args = (List *) PG_GETARG_POINTER(2);
	int			varRelid = PG_GETARG_INT32(3);
	VariableStatData vardata;
	Node	   *other;
	bool		varonleft;

	Selectivity	selec = DEFAULT_CONTAIN_SEL;

	/*
	 * If expression is not (variable op something) or (something op
	 * variable), then punt and return a default estimate.
	 */
	if (!get_restriction_variable(root, args, varRelid,
								  &vardata, &other, &varonleft))
		PG_RETURN_FLOAT8(selec);

	/*
	 * Can't do anything useful if the something is not a constant, either.
	 */
	if (!IsA(other, Const))
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(selec);
	}

	/*
	 * The "@>" and "<@" operators are strict, so we can cope with a NULL
	 * constant right away.
	 */
	if (((Const *) other)->constisnull)
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(0.0);
	}
#define	OID_JSONB_CONTAINS_OP	3246
#define	OID_JSONB_CONTAINED_OP	3250
	/*
	 * If var is on the right, commute the operator, so that we can assume the
	 * var is on the left in what follows.
	 */
	if (!varonleft)
	{
		if (operator == OID_JSONB_CONTAINS_OP)
			operator = OID_JSONB_CONTAINED_OP;
		else if (operator == OID_JSONB_CONTAINED_OP)
			operator = OID_JSONB_CONTAINS_OP;
	}

	/*
	 * OK, there's a Var and a Const we're dealing with here.  We need the
	 * Const to be a JSONB value, just like the column, else we can't do
	 * anything useful.  (Such cases will likely fail at runtime, but here
	 * we'd rather just return a default estimate.)
	 */
	if (((Const *) other)->consttype == JSONBOID)
	{
		selec = calc_jsonbcontsel(&vardata, ((Const *) other)->constvalue,
								  operator);
	}

	ReleaseVariableStats(vardata);

	CLAMP_PROBABILITY(selec);

	PG_RETURN_FLOAT8(selec);
}



/*
 * Calculate selectivity for "jsonbcolumn @> const" or "jsonbcolumn <@ const"
 * based on the statistics
 *
 * This function is mainly responsible for extracting the pg_statistic data
 * to be used; we then pass the problem on to mcelem_jsonb_selec().
 */
static Selectivity
calc_jsonbcontsel(VariableStatData *vardata, Datum constval, Oid operator)
{
	Selectivity selec;
	TypeCacheEntry *typentry;
	FmgrInfo   *cmpfunc;
	Jsonb	   *jsonb;

	/* Get comparison function for TEXT (because we store paths as text) */
	typentry = lookup_type_cache(TEXTOID, TYPECACHE_CMP_PROC_FINFO);
	if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
		return DEFAULT_CONTAIN_SEL;
	cmpfunc = &typentry->cmp_proc_finfo;

	/*
	 * The caller made sure the const is a JSONB too, so get it now.
	 */
	jsonb = DatumGetJsonbP(constval);

	if (HeapTupleIsValid(vardata->statsTuple))
	{
		Form_pg_statistic stats;
		AttStatsSlot sslot;

		stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);

		/* MCELEM will be an array of same type as column */
		if (get_attstatsslot(&sslot, vardata->statsTuple,
							 STATISTIC_KIND_MCELEM, InvalidOid,
							 ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
		{
			/* Use the most-common-elements slot for the array Var. */
			selec = mcelem_jsonb_selec(jsonb, typentry,
									   sslot.values, sslot.nvalues,
									   sslot.numbers, sslot.nnumbers,
									   operator, cmpfunc);

			free_attstatsslot(&sslot);
		}
		else
		{
			/* No most-common-elements info, so do without */
			selec = mcelem_jsonb_selec(jsonb, typentry,
									   NULL, 0, NULL, 0,
									   operator, cmpfunc);
		}

		/*
		 * MCE stats count only non-null rows, so adjust for null rows.
		 */
		selec *= (1.0 - stats->stanullfrac);
	}
	else
	{
		/* No stats at all, so do without */
		selec = mcelem_jsonb_selec(jsonb, typentry,
								   NULL, 0, NULL, 0,
								   operator, cmpfunc);
		/* we assume no nulls here, so no stanullfrac correction */
	}

	/* If constant was toasted, release the copy we made */
	if (PointerGetDatum(jsonb) != constval)
		pfree(jsonb);

	return selec;
}

/*
 * Array selectivity estimation based on most common elements statistics
 *
 * This function just deconstructs and sorts the array constant's contents,
 * and then passes the problem on to mcelem_array_contain_overlap_selec or
 * mcelem_array_contained_selec depending on the operator.
 */
static Selectivity
mcelem_jsonb_selec(Jsonb *jsonb, TypeCacheEntry *typentry,
				   Datum *mcelem, int nmcelem,
				   float4 *numbers, int nnumbers,
				   Oid operator, FmgrInfo *cmpfunc)
{
	int			i;
	Selectivity selec = 0.0;
	List	   *paths;
	ListCell   *lc;

	paths = jsonb_get_paths(jsonb);

	foreach (lc, paths)
	{
		String     *path = (String *)lfirst(lc);
        text       *val = cstring_to_text(path->val);

		for (i = 0; i < nmcelem; i++)
		{
			Datum	c = FunctionCall2Coll(cmpfunc, DEFAULT_COLLATION_OID, mcelem[i], (Datum)val);

			if (!c)
			{
				selec += numbers[i];
				break;
			}
		}
	}

	list_free(paths);

	return selec;
}

static List *
jsonb_get_paths(Jsonb *jb)
{
       JsonbIterator *it;
       JsonbValue      v;
       int                     i;
       int                     r;
       List       *paths = NIL;
       bool ret = true;

       int                     depth = 0;
       int                     maxlen = 256;
       char      **keys = palloc0(maxlen * sizeof(void*));

       StringInfoData buffer;

       initStringInfo(&buffer);

       it = JsonbIteratorInit(&jb->root);

       while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
       {
               /* we're only interested in keys at this point */
               switch (r)
               {
                       case WJB_BEGIN_OBJECT:
                               ret = true;
                               depth++;
                               break;

                       case WJB_END_OBJECT:

							   if (! ret)
							      break;

                               ret = false;

                               resetStringInfo(&buffer);

                               for (i = 0; i < depth; i++)
                               {
                                       if (i > 0)
                                               appendStringInfoChar(&buffer, ':');
                                       appendStringInfoString(&buffer, keys[i]);
                               }
                               appendStringInfoChar(&buffer, '\0');

elog(WARNING, "path = '%s'", buffer.data);

                               paths = lappend(paths, makeString(pstrdup(buffer.data)));

                               pfree(keys[depth-1]);
                               depth--;
                               break;

                       case WJB_KEY:
                               keys[depth-1] = palloc0(v.val.string.len+1);
                               memcpy(keys[depth-1], v.val.string.val, v.val.string.len);
                               break;

                       case WJB_BEGIN_ARRAY:
                       case WJB_ELEM:
                       case WJB_VALUE:
                       case WJB_END_ARRAY:
                               break;

                       default:
                               elog(ERROR, "invalid JsonbIteratorNext rc: %d", r);
               }
       }

       return paths;
}
