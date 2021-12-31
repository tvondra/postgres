/*-------------------------------------------------------------------------
 *
 * jsonb_typanalyze.c
 *	  Functions for gathering statistics from jsonb columns
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * Functions in this module are used to analyze contents of JSONB columns
 * and build optimizer statistics. In principle we extract paths from all
 * sampled documents and calculate the usual statistics (MCV, histogram)
 * for each path - in principle each path is treated as a column.
 *
 * Because we're not enforcing any JSON schema, the documents may differ
 * a lot - the documents may contain large number of different keys, the
 * types of values may be entirely different, etc. This makes it more
 * challenging than building stats for regular columns. For example not
 * only do we need to decide which values to keep in the MCV, but also
 * which paths to keep (in case the documents are so variable we can't
 * keep all paths).
 *
 * The statistics is stored in pg_statistic, in a slot with a new stakind
 * value (STATISTIC_KIND_JSON). The statistics is serialized as an array
 * of JSONB values, eash element storing statistics for one path.
 *
 * For each path, we store the following keys:
 *
 * - path         - path this stats is for, serialized as jsonpath
 * - freq         - frequency of documents containing this path
 * - json         - the regular per-column stats (MCV, histogram, ...)
 * - freq_null    - frequency of JSON null values
 * - freq_array   - frequency of JSON array values
 * - freq_object  - frequency of JSON object values
 * - freq_string  - frequency of JSON string values
 * - freq_numeric - frequency of JSON numeric values
 *
 * This is stored in the stavalues array.
 *
 * The per-column stats (stored in the "json" key) have additional internal
 * structure, to allow storing multiple stakind types (histogram, mcv). See
 * jsonAnalyzeMakeScalarStats for details.
 *
 *
 * XXX There's additional stuff (prefix, length stats) stored in the first
 * two elements, I think.
 *
 * XXX It's a bit weird the "regular" stats are stored in the "json" key,
 * while the JSON stats (frequencies of different JSON types) are right
 * at the top level.
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_typanalyze.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "access/hash.h"
#include "access/detoast.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/json_selfuncs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

typedef struct JsonPathEntry JsonPathEntry;

/*
 * Element of a path in the JSON document (i.e. not jsonpath). Elements
 * are linked together to build longer paths.
 *
 * XXX We need entry+lenth because JSON path elements may contain null
 * bytes, I guess?
 */
struct JsonPathEntry
{
	JsonPathEntry  *parent;
	const char	   *entry;		/* element of the path as a string */
	int				len;		/* length of entry string (may be 0) */
	uint32			hash;		/* hash of the whole path (with parent) */
};

/*
 * A path is simply a pointer to the last element (we can traverse to
 * the top easily).
 */
typedef JsonPathEntry *JsonPath;

/* An array containing a dynamic number of JSON values. */
typedef struct JsonValues
{
	Datum	   *buf;
	int			count;
	int			allocated;
} JsonValues;

/*
 * Scalar statistics built for an array of values, extracted from a JSON
 * document (for one particular path).
 *
 * XXX The array can contain values of different JSON type, probably?
 */
typedef struct JsonScalarStats
{
	JsonValues		values;
	VacAttrStats	stats;
} JsonScalarStats;

/*
 * Statistics calculated for a set of values.
 *
 *
 * XXX This seems rather complicated and needs simplification. We're not
 * really using all the various JsonScalarStats bits, there's a lot of
 * duplication (e.g. each JsonScalarStats contains it's own array, which
 * has a copy of data from the one in "jsons"). Some of it is defined as
 * a typedef, but booleans have inline struct.
 */
typedef struct JsonValueStats
{
	JsonScalarStats	jsons;		/* stats for all JSON types together */

	/* XXX used only with JSON_ANALYZE_SCALARS defined */
	JsonScalarStats	strings;	/* stats for JSON strings */
	JsonScalarStats	numerics;	/* stats for JSON numerics */

	/* stats for booleans */
	struct
	{
		int		ntrue;
		int		nfalse;
	}				booleans;

	int				nnulls;		/* number of JSON null values */
	int				nobjects;	/* number of JSON objects */
	int				narrays;	/* number of JSON arrays */

	JsonScalarStats	lens;		/* stats of object lengths */
	JsonScalarStats	arrlens;	/* stats of array lengths */
} JsonValueStats;

/* ??? */
typedef struct JsonPathAnlStats
{
	JsonPathEntry		path;
	JsonValueStats		vstats;
	Jsonb			   *stats;
	char			   *pathstr;
	double				freq;
	int					depth;
	JsonPathCollectStatus	collected;

	JsonPathEntry	  **entries;
	int					nentries;
} JsonPathAnlStats;

/* various bits needed while analyzing JSON */
typedef struct JsonAnalyzeContext
{
	VacAttrStats		   *stats;
	MemoryContext			mcxt;
	AnalyzeAttrFetchFunc	fetchfunc;
	HTAB				   *pathshash;
	JsonPathAnlStats	   *root;
	JsonPathAnlStats	  **paths;
	const char			  **collectedPaths;
	int						npaths;
	double					totalrows;
	double					total_width;
	int						samplerows;
	int						target;
	int						null_cnt;
	int						analyzed_cnt;
	int						maxdepth;
	bool					scalarsOnly;
} JsonAnalyzeContext;

/*
 * JsonPathMatch
 *		Determine when two JSON paths (list of JsonPathEntry) match.
 *
 * XXX Sould be JsonPathEntryMatch as it deals with JsonPathEntry nodes
 * not whole paths, no?
 *
 * XXX Seems a bit silly to return int, when the return statement only
 * really returns bool (because of how it compares paths). It's not really
 * a comparator for sorting, for example.
 */
static int
JsonPathMatch(const void *key1, const void *key2, Size keysize)
{
	const JsonPathEntry *path1 = key1;
	const JsonPathEntry *path2 = key2;

	return path1->parent != path2->parent ||
		   path1->len != path2->len ||
		   (path1->len > 0 &&
			strncmp(path1->entry, path2->entry, path1->len));
}

/*
 * JsonPathHash
 *		Calculate hash of the path entry.
 *
 * XXX Again, maybe JsonPathEntryHash would be a better name?
 *
 * XXX Maybe should call JsonPathHash on the parent, instead of looking
 * at the field directly. Could easily happen we have not calculated it
 * yet, I guess.
 */
static uint32
JsonPathHash(const void *key, Size keysize)
{
	const JsonPathEntry	   *path = key;

	/* XXX Call JsonPathHash instead of direct access? */
	uint32					hash = path->parent ? path->parent->hash : 0;

	hash = (hash << 1) | (hash >> 31);
	hash ^= path->len < 0 ? 0 : DatumGetUInt32(
					hash_any((const unsigned char *) path->entry, path->len));

	return hash;
}

static inline void
jsonAnalyzeReportInvalidPath(const char *path)
{
	elog(ERROR, "invalid json path '%s'", path);
}

static inline const char *
jsonAnalyzeNormalizePathEntry(StringInfo str, const char *c)
{
	if (*c == '$' || *c == '.')
		return NULL;

	if (*c == '*' || *c == '%' || *c == '#')
	{
		appendStringInfoCharMacro(str, *c);
		return *c != '#' && c[1] ? NULL : c + 1;
	}

	appendStringInfoCharMacro(str, '"');

	if (*c == '"')
	{
		for (c++; *c && *c != '"'; c++)
		{
			if (*c == '\\')
			{
				appendStringInfoCharMacro(str, '\\');
				if (!*c++)
					return NULL;
			}
			appendStringInfoCharMacro(str, *c);
		}

		if (*c++ != '"')
			return NULL;
	}
	else
	{
		for (; *c && *c != '.'; c++)
			if (isalnum(*c) || *c == '_')
				appendStringInfoCharMacro(str, *c);
			else
				return NULL;
	}

	appendStringInfoCharMacro(str, '"');

	return c;
}

static char *
jsonAnalyzeNormalizePath(const char *path)
{
	StringInfoData	str;
	const char	   *c;

	str.maxlen = strlen(path) + 10;
	str.data = (char *) palloc(str.maxlen);

	resetStringInfo(&str);

	c = path;

	if (*c == '$')
	{
		c++;
		if (*c && *c++ != '.')
			jsonAnalyzeReportInvalidPath(path);
	}

	appendStringInfoCharMacro(&str, '$');

	while (*c)
	{
		appendStringInfoCharMacro(&str, '.');

		if (!(c = jsonAnalyzeNormalizePathEntry(&str, c)))
			jsonAnalyzeReportInvalidPath(path);

		if (*c)
		{
			if (c[0] != '.' || !c[1])
				jsonAnalyzeReportInvalidPath(path);
			c++;
		}
	}

	return str.data;
}

typedef enum
{
	JSPM_NOT_MATCHED,
	JSPM_MATCHED,
	JSPM_SUPERPATH,
} JsonPathMatchResult;

static JsonPathMatchResult
jsonAnalyzeComparePath(const char *pattern, const char *path, int pathlen)
{
	const char	   *c1;
	const char	   *c2;
	const char	   *c2end = path + pathlen;
	bool			entry = false;

	for (c1 = pattern, c2 = path; *c1 && c2 < c2end && *c1 == *c2; c1++, c2++)
	{
		if (*c1 == '"')
			entry ^= true;
		else if (entry && *c1 == '\\' && *++c1 != *++c2)
			break;
	}

	if (entry)
		return JSPM_NOT_MATCHED;

	if (!*c1)
		return c2 < c2end ? JSPM_NOT_MATCHED : JSPM_MATCHED;

	if (*c1 == '*')
		return JSPM_MATCHED;

	if (c1[0] == '.' && c1[1] == '*' && c2 >= c2end)
		return JSPM_MATCHED;

	if (*c1 == '%')
	{
		if (*c2++ != '"')
			return JSPM_NOT_MATCHED;

		while (c2 < c2end && *c2 != '"')
		{
			if (*c2 == '\\')
				c2++;
			c2++;
		}

		return *c2 == '"' && c2 == c2end ? JSPM_MATCHED : JSPM_NOT_MATCHED;
	}

	if (*c1 == '.' && c2 == c2end)
		return JSPM_SUPERPATH;

	return JSPM_NOT_MATCHED;
}

JsonPathCollectStatus
jsonAnalyzePathIsCollected(const char **paths, const char *path, int pathlen)
{
	const char			  **ppath;
	JsonPathCollectStatus	result = JSPCS_NOT_COLLECTED;

	for (ppath = paths; *ppath; ppath++)
	{
		switch (jsonAnalyzeComparePath(*ppath, path, pathlen))
		{
			case JSPM_NOT_MATCHED:
				break;
			case JSPM_MATCHED:
				return JSPCS_COLLECTED;
			case JSPM_SUPERPATH:
				result = JSPCS_COLLECTED_SUPERPATH;
				break;
		}
	}

	return result;
}

/*
 * jsonAnalyzeAddPath
 *		Add an entry for a JSON path to the working list of statistics.
 *
 * Returns a pointer to JsonPathAnlStats (which might have already existed
 * if the path was in earlier document), which can then be populated or
 * updated.
 */
static inline JsonPathAnlStats *
jsonAnalyzeAddPath(JsonAnalyzeContext *ctx, JsonPath path)
{
	JsonPathAnlStats   *stats;
	bool				found;

	path->hash = JsonPathHash(path, 0);

	/* XXX See if we already saw this path earlier. */
	stats = hash_search_with_hash_value(ctx->pathshash, path, path->hash,
										HASH_ENTER, &found);

	/*
	 * Nope, it's the first time we see this path, so initialize all the
	 * fields (path string, counters, ...).
	 */
	if (!found)
	{
		JsonPathAnlStats   *parent = (JsonPathAnlStats *) stats->path.parent;
		const char		   *ppath = parent->pathstr;

		path = &stats->path;

		/* Is it valid path? If not, we treat it as $.# */
		if (path->len >= 0)
		{
			StringInfoData	si;
			MemoryContext	oldcxt = MemoryContextSwitchTo(ctx->mcxt);

			initStringInfo(&si);

			path->entry = pnstrdup(path->entry, path->len);

			appendStringInfo(&si, "%s.", ppath);
			escape_json(&si, path->entry);

			stats->pathstr = si.data;

			MemoryContextSwitchTo(oldcxt);
		}
		else
		{
			int pathstrlen = strlen(ppath) + 3;
			stats->pathstr = MemoryContextAlloc(ctx->mcxt, pathstrlen);
			snprintf(stats->pathstr, pathstrlen, "%s.#", ppath);
		}

		/* initialize the stats counter for this path entry */
		memset(&stats->vstats, 0, sizeof(JsonValueStats));
		stats->stats = NULL;
		stats->freq = 0.0;
		stats->depth = parent->depth + 1;
		stats->entries = NULL;
		stats->nentries = 0;

		/* XXX Seems strange. Should we even add the path in this case? */
		if (stats->depth > ctx->maxdepth)
			ctx->maxdepth = stats->depth;

		stats->collected = jsonAnalyzePathIsCollected(ctx->collectedPaths,
													  stats->pathstr,
													  strlen(stats->pathstr));
	}

	return stats;
}

/*
 * JsonValuesAppend
 *		Add a JSON value to the dynamic array (enlarge it if needed).
 *
 * XXX This is likely one of the problems - the documents may be pretty
 * large, with a lot of different values for each path. At that point
 * it's problematic to keep all of that in memory at once. So maybe we
 * need to introduce some sort of compaction (e.g. we could try
 * deduplicating the values), limit on size of the array or something.
 */
static inline void
JsonValuesAppend(JsonValues *values, Datum value, int initialSize)
{
	if (values->count >= values->allocated)
	{
		if (values->allocated)
		{
			values->allocated = values->allocated * 2;
			values->buf = repalloc(values->buf,
									sizeof(values->buf[0]) * values->allocated);
		}
		else
		{
			values->allocated = initialSize;
			values->buf = palloc(sizeof(values->buf[0]) * values->allocated);
		}
	}

	values->buf[values->count++] = value;
}

/*
 * jsonAnalyzeJsonValue
 *		Process a value extracted from the document (for a given path).
 */
static inline void
jsonAnalyzeJsonValue(JsonAnalyzeContext *ctx, JsonPathAnlStats *stats,
					 JsonbValue *jv)
{
	JsonValueStats	   *vstats = &stats->vstats;
	JsonScalarStats	   *sstats;
	JsonbValue		   *jbv;
	JsonbValue			jbvtmp;
	Datum				value;

	if (stats->collected == JSPCS_NOT_COLLECTED)
		return;

	/* ??? */
	if (ctx->scalarsOnly && jv->type == jbvBinary)
	{
		if (JsonContainerIsObject(jv->val.binary.data))
			jbv = JsonValueInitObject(&jbvtmp, 0, 0);
		else
		{
			Assert(JsonContainerIsArray(jv->val.binary.data));
			jbv = JsonValueInitArray(&jbvtmp, 0, 0, false);
		}
	}
	else
		jbv = jv;

	/* always add it to the "global" JSON stats, shared by all types */
	if (stats->collected == JSPCS_COLLECTED)
		JsonValuesAppend(&vstats->jsons.values,
						JsonbPGetDatum(JsonbValueToJsonb(jbv)),
						ctx->target);
	else
		vstats->jsons.values.count++;

	/*
	 * Maybe also update the type-specific counters.
	 *
	 * XXX The mix of break/return statements in this block is really
	 * confusing.
	 */
	switch (jv->type)
	{
		case jbvNull:
			++vstats->nnulls;
			return;

		case jbvBool:
			++*(jv->val.boolean ? &vstats->booleans.ntrue
								: &vstats->booleans.nfalse);
			return;

		case jbvString:
#ifdef JSON_ANALYZE_SCALARS
			sstats = &vstats->strings;
			value = PointerGetDatum(
						cstring_to_text_with_len(jv->val.string.val,
												 jv->val.string.len));
			break;
#else
			vstats->strings.values.count++;
			return;
#endif

		case jbvNumeric:
#ifdef JSON_ANALYZE_SCALARS
			sstats = &vstats->numerics;
			value = PointerGetDatum(jv->val.numeric);
			break;
#else
			vstats->numerics.values.count++;
			return;
#endif

		case jbvBinary:
			if (JsonContainerIsObject(jv->val.binary.data))
			{
				uint32 size = JsonContainerSize(jv->val.binary.data);
				value = DatumGetInt32(size);
				sstats = &vstats->lens;
				vstats->nobjects++;
				break;
			}
			else if (JsonContainerIsArray(jv->val.binary.data))
			{
				uint32 size = JsonContainerSize(jv->val.binary.data);
				value = DatumGetInt32(size);
				sstats = &vstats->lens;
				vstats->narrays++;
				JsonValuesAppend(&vstats->arrlens.values, value, ctx->target);
				break;
			}
			return;

		default:
			elog(ERROR, "invalid scalar json value type %d", jv->type);
			break;
	}

	if (stats->collected == JSPCS_COLLECTED)
		JsonValuesAppend(&sstats->values, value, ctx->target);
}

/*
 * jsonAnalyzeJson
 *		Parse the JSON document and build/update stats.
 *
 * XXX The name seems a bit weird, with the two json bits.
 *
 * XXX The param is either NULL, (char *) -1, or a pointer 
 */
static void
jsonAnalyzeJson(JsonAnalyzeContext *ctx, Jsonb *jb, void *param)
{
	JsonbValue			jv;
	JsonbIterator	   *it;
	JsonbIteratorToken	tok;
	JsonPathAnlStats   *target = (JsonPathAnlStats *) param;
	JsonPathAnlStats   *stats = ctx->root;
	JsonPath			path = &stats->path;
	JsonPathEntry		entry;
	bool				scalar = false;

	if ((!target || target == stats) &&
		!JB_ROOT_IS_SCALAR(jb))
		jsonAnalyzeJsonValue(ctx, stats, JsonValueInitBinary(&jv, jb));

	it = JsonbIteratorInit(&jb->root);

	while ((tok = JsonbIteratorNext(&it, &jv, true)) != WJB_DONE)
	{
		switch (tok)
		{
			case WJB_BEGIN_OBJECT:
				entry.entry = NULL;
				entry.len = -1;
				entry.parent = path;
				path = &entry;

				break;

			case WJB_END_OBJECT:
				stats = (JsonPathAnlStats *)(path = path->parent);
				break;

			case WJB_BEGIN_ARRAY:
				if (!(scalar = jv.val.array.rawScalar))
				{
					entry.entry = NULL;
					entry.len = -1;
					entry.parent = path;
					path = &(stats = jsonAnalyzeAddPath(ctx, &entry))->path;
				}
				break;

			case WJB_END_ARRAY:
				if (!scalar)
					stats = (JsonPathAnlStats *)(path = path->parent);
				break;

			case WJB_KEY:
				entry.entry = jv.val.string.val;
				entry.len = jv.val.string.len;
				entry.parent = path->parent;
				path = &(stats = jsonAnalyzeAddPath(ctx, &entry))->path;
				break;

			case WJB_VALUE:
			case WJB_ELEM:
				if (!target || target == stats)
					jsonAnalyzeJsonValue(ctx, stats, &jv);

				/* XXX not sure why we're doing this? */
				if (stats->collected != JSPCS_NOT_COLLECTED &&
					jv.type == jbvBinary)
				{
					/* recurse into container */
					JsonbIterator *it2 = JsonbIteratorInit(jv.val.binary.data);

					it2->parent = it;
					it = it2;
				}
				break;

			default:
				break;
		}
	}
}

/*
 * jsonAnalyzeJsonSubpath
 *		???
 */
static void
jsonAnalyzeJsonSubpath(JsonAnalyzeContext *ctx, JsonPathAnlStats *pstats,
					   JsonbValue *jbv, int n)
{
	JsonbValue	scalar;
	int			i;

	for (i = n; i < pstats->depth; i++)
	{
		JsonPathEntry  *entry = pstats->entries[i];
		JsonbContainer *jbc = jbv->val.binary.data;
		JsonbValueType	type = jbv->type;

		if (i > n)
			pfree(jbv);

		if (type != jbvBinary)
			return;

		if (entry->len == -1)
		{
			JsonbIterator	   *it;
			JsonbIteratorToken	r;
			JsonbValue			elem;

			if (!JsonContainerIsArray(jbc) || JsonContainerIsScalar(jbc))
				return;

			it = JsonbIteratorInit(jbc);

			while ((r = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
			{
				if (r == WJB_ELEM)
					jsonAnalyzeJsonSubpath(ctx, pstats, &elem, i + 1);
			}

			return;
		}
		else
		{
			if (!JsonContainerIsObject(jbc))
				return;

			jbv = findJsonbValueFromContainerLen(jbc, JB_FOBJECT,
												 entry->entry, entry->len);

			if (!jbv)
				return;
		}
	}

	if (i == n &&
		jbv->type == jbvBinary &&
		JsonbExtractScalar(jbv->val.binary.data, &scalar))
		jbv = &scalar;

	jsonAnalyzeJsonValue(ctx, pstats, jbv);

	if (i > n)
		pfree(jbv);
}

/*
 * jsonAnalyzeJsonPath
 *		???
 */
static void
jsonAnalyzeJsonPath(JsonAnalyzeContext *ctx, Jsonb *jb, void *param)
{
	JsonPathAnlStats   *pstats = (JsonPathAnlStats *) param;
	JsonbValue			jbvtmp;
	JsonbValue		   *jbv = JsonValueInitBinary(&jbvtmp, jb);
	JsonPath			path;

	if (!pstats->entries)
	{
		int i;

		pstats->entries = MemoryContextAlloc(ctx->mcxt,
									sizeof(*pstats->entries) * pstats->depth);

		for (path = &pstats->path, i = pstats->depth - 1;
			 path->parent && i >= 0;
			 path = path->parent, i--)
			pstats->entries[i] = path;
	}

	jsonAnalyzeJsonSubpath(ctx, pstats, jbv, 0);
}

static Datum
jsonAnalyzePathFetch(VacAttrStatsP stats, int rownum, bool *isnull)
{
	*isnull = false;
	return stats->exprvals[rownum];
}

/*
 * jsonAnalyzePathValues
 *		Calculate per-column statistics for values for a single path.
 *
 * We have already accumulated all the values for the path, so we simply
 * call the typanalyze function for the proper data type, and then
 * compute_stats (which points to compute_scalar_stats or so).
 */
static void
jsonAnalyzePathValues(JsonAnalyzeContext *ctx, JsonScalarStats *sstats,
					  Oid typid, double freq)
{
	JsonValues			   *values = &sstats->values;
	VacAttrStats		   *stats = &sstats->stats;
	FormData_pg_attribute	attr;
	FormData_pg_type		type;
	int						i;

	if (!sstats->values.count)
		return;

	get_typlenbyvalalign(typid, &type.typlen, &type.typbyval, &type.typalign);

	attr.attstattarget = ctx->target;

	stats->attr = &attr;
	stats->attrtypid = typid;
	stats->attrtypmod = -1;
	stats->attrtype = &type;
	stats->anl_context = ctx->stats->anl_context;

	stats->exprvals = values->buf;

	/* XXX Do we need to initialize all slots? */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	std_typanalyze(stats);

	stats->compute_stats(stats, jsonAnalyzePathFetch,
						 values->count,
						 ctx->totalrows / ctx->samplerows * values->count);

	/*
	 * We've only kept the non-null values, so compute_stats will always
	 * leave this as 1.0. But we have enough info to calculate the correct
	 * value.
	 */
	stats->stanullfrac = (float4)(1.0 - freq);

	/*
	 * Similarly, we need to correct the MCV frequencies, becuse those are
	 * also calculated only from the non-null values. All we need to do is
	 * simply multiply that with the non-NULL frequency.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		if (stats->stakind[i] == STATISTIC_KIND_MCV)
		{
			int j;
			for (j = 0; j < stats->numnumbers[i]; j++)
				stats->stanumbers[i][j] *= freq;
		}
	}
}

/*
 * jsonAnalyzeMakeScalarStats
 *		Serialize scalar stats into a JSON representation.
 *
 * We simply produce a JSON document with a list of predefined keys:
 *
 * - nullfrac
 * - distinct
 * - width
 * - correlation
 * - mcv or histogram
 *
 * For the mcv / histogram, we store a nested values / numbers.
 */
static JsonbValue *
jsonAnalyzeMakeScalarStats(JsonbParseState **ps, const char *name,
							const VacAttrStats *stats)
{
	JsonbValue	val;
	int			i;
	int			j;

	pushJsonbKey(ps, &val, name);

	pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);

	pushJsonbKeyValueFloat(ps, &val, "nullfrac", stats->stanullfrac);
	pushJsonbKeyValueFloat(ps, &val, "distinct", stats->stadistinct);
	pushJsonbKeyValueInteger(ps, &val, "width", stats->stawidth);

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		if (!stats->stakind[i])
			break;

		switch (stats->stakind[i])
		{
			case STATISTIC_KIND_MCV:
				pushJsonbKey(ps, &val, "mcv");
				break;

			case STATISTIC_KIND_HISTOGRAM:
				pushJsonbKey(ps, &val, "histogram");
				break;

			case STATISTIC_KIND_CORRELATION:
				pushJsonbKeyValueFloat(ps, &val, "correlation",
									   stats->stanumbers[i][0]);
				continue;

			default:
				elog(ERROR, "unexpected stakind %d", stats->stakind[i]);
				break;
		}

		pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);

		if (stats->numvalues[i] > 0)
		{
			pushJsonbKey(ps, &val, "values");
			pushJsonbValue(ps, WJB_BEGIN_ARRAY, NULL);
			for (j = 0; j < stats->numvalues[i]; j++)
			{
				Datum v = stats->stavalues[i][j];
				if (stats->attrtypid == JSONBOID)
					pushJsonbElemBinary(ps, &val, DatumGetJsonbP(v));
				else if (stats->attrtypid == TEXTOID)
					pushJsonbElemText(ps, &val, DatumGetTextP(v));
				else if (stats->attrtypid == NUMERICOID)
					pushJsonbElemNumeric(ps, &val, DatumGetNumeric(v));
				else if (stats->attrtypid == INT4OID)
					pushJsonbElemInteger(ps, &val, DatumGetInt32(v));
				else
					elog(ERROR, "unexpected stat value type %d",
						 stats->attrtypid);
			}
			pushJsonbValue(ps, WJB_END_ARRAY, NULL);
		}

		if (stats->numnumbers[i] > 0)
		{
			pushJsonbKey(ps, &val, "numbers");
			pushJsonbValue(ps, WJB_BEGIN_ARRAY, NULL);
			for (j = 0; j < stats->numnumbers[i]; j++)
				pushJsonbElemFloat(ps, &val, stats->stanumbers[i][j]);
			pushJsonbValue(ps, WJB_END_ARRAY, NULL);
		}

		pushJsonbValue(ps, WJB_END_OBJECT, NULL);
	}

	return pushJsonbValue(ps, WJB_END_OBJECT, NULL);
}

/*
 * jsonAnalyzeBuildPathStats
 *		Serialize statistics for a particular json path.
 *
 * This includes both the per-column stats (stored in "json" key) and the
 * JSON specific stats (like frequencies of different object types).
 */
static Jsonb *
jsonAnalyzeBuildPathStats(JsonPathAnlStats *pstats)
{
	const JsonValueStats *vstats = &pstats->vstats;
	float4				freq = pstats->freq;
	bool				full = !!pstats->path.parent;
	JsonbValue			val;
	JsonbValue		   *jbv;
	JsonbParseState	   *ps = NULL;

	pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

	pushJsonbKeyValueString(&ps, &val, "path", pstats->pathstr);

	pushJsonbKeyValueFloat(&ps, &val, "freq", freq);

	pushJsonbKeyValueFloat(&ps, &val, "freq_null",
						   freq * vstats->nnulls /
								  vstats->jsons.values.count);

	pushJsonbKeyValueFloat(&ps, &val, "freq_boolean",
						   freq * (vstats->booleans.nfalse +
								   vstats->booleans.ntrue) /
								  vstats->jsons.values.count);

	pushJsonbKeyValueFloat(&ps, &val, "freq_string",
						   freq * vstats->strings.values.count /
								  vstats->jsons.values.count);

	pushJsonbKeyValueFloat(&ps, &val, "freq_numeric",
						   freq * vstats->numerics.values.count /
								  vstats->jsons.values.count);

	pushJsonbKeyValueFloat(&ps, &val, "freq_array",
						   freq * vstats->narrays /
								  vstats->jsons.values.count);

	pushJsonbKeyValueFloat(&ps, &val, "freq_object",
						   freq * vstats->nobjects /
								  vstats->jsons.values.count);

	/*
	 * XXX not sure why we keep length and array length stats at this level.
	 * Aren't those covered by the per-column stats? We certainly have
	 * frequencies for array elements etc.
	 */
	if (pstats->vstats.lens.values.count)
		jsonAnalyzeMakeScalarStats(&ps, "length", &vstats->lens.stats);

	if (pstats->path.len == -1)
	{
		JsonPathAnlStats *parent = (JsonPathAnlStats *) pstats->path.parent;

		pushJsonbKeyValueFloat(&ps, &val, "avg_array_length",
							   (float4) vstats->jsons.values.count /
										parent->vstats.narrays);

		if (parent->vstats.arrlens.values.count)
			jsonAnalyzeMakeScalarStats(&ps, "array_length",
									   &parent->vstats.arrlens.stats);
	}

	if (full)
	{
		if (pstats->collected == JSPCS_COLLECTED)
		{
#ifdef JSON_ANALYZE_SCALARS
			jsonAnalyzeMakeScalarStats(&ps, "string", &vstats->strings.stats);
			jsonAnalyzeMakeScalarStats(&ps, "numeric", &vstats->numerics.stats);
#endif
			jsonAnalyzeMakeScalarStats(&ps, "json", &vstats->jsons.stats);
		}
		else
		{
			pushJsonbKey(&ps, &val, "json");
			pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);
			pushJsonbKeyValueFloat(&ps, &val, "nullfrac", 1.0 - pstats->freq);
			pushJsonbKeyValueFloat(&ps, &val, "distinct", 0.0);
			pushJsonbKeyValueInteger(&ps, &val, "width", 0.0);
			pushJsonbValue(&ps, WJB_END_OBJECT, NULL);
		}
	}

	jbv = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(jbv);
}

/*
 * jsonAnalyzeCalcPathFreq
 *		Calculate path frequency, i.e. how many documents contain this path.
 */
static void
jsonAnalyzeCalcPathFreq(JsonAnalyzeContext *ctx, JsonPathAnlStats *pstats)
{
	JsonPathAnlStats  *parent = (JsonPathAnlStats *) pstats->path.parent;

	if (parent)
	{
		pstats->freq = parent->freq *
			(pstats->path.len == -1 ? parent->vstats.narrays
									: pstats->vstats.jsons.values.count) /
			parent->vstats.jsons.values.count;

		CLAMP_PROBABILITY(pstats->freq);
	}
	else
		pstats->freq = (double) ctx->analyzed_cnt / ctx->samplerows;
}

/*
 * jsonAnalyzePath
 *		Build statistics for values accumulated for this path.
 *
 * We're done with accumulating values for this path, so calculate the
 * statistics for the various arrays.
 *
 * XXX I wonder if we could introduce some simple heuristict on which
 * paths to keep, similarly to what we do for MCV lists. For example a
 * path that occurred just once is not very interesting, so we could
 * decide to ignore it and not build the stats. Although that won't
 * save much, because there'll be very few values accumulated.
 */
static void
jsonAnalyzePath(JsonAnalyzeContext *ctx, JsonPathAnlStats *pstats)
{
	MemoryContext		oldcxt;

	jsonAnalyzeCalcPathFreq(ctx, pstats);

	if (pstats->collected == JSPCS_NOT_COLLECTED)
		return;

	if (pstats->collected == JSPCS_COLLECTED)
	{
		JsonValueStats	   *vstats = &pstats->vstats;

		jsonAnalyzePathValues(ctx, &vstats->jsons, JSONBOID, pstats->freq);
		jsonAnalyzePathValues(ctx, &vstats->lens, INT4OID,
							  pstats->freq * vstats->lens.values.count /
											 vstats->jsons.values.count);
		jsonAnalyzePathValues(ctx, &vstats->arrlens, INT4OID,
							  pstats->freq * vstats->arrlens.values.count /
											 vstats->jsons.values.count);
#ifdef JSON_ANALYZE_SCALARS
		jsonAnalyzePathValues(ctx, &vstats->strings, TEXTOID, pstats->freq);
		jsonAnalyzePathValues(ctx, &vstats->numerics, NUMERICOID, pstats->freq);
#endif
	}

	oldcxt = MemoryContextSwitchTo(ctx->stats->anl_context);
	pstats->stats = jsonAnalyzeBuildPathStats(pstats);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * JsonPathStatsCompare
 *		Compare two path stats (by path string).
 *
 * We store the stats sorted by path string, and this is the comparator.
 */
static int
JsonPathStatsCompare(const void *pv1, const void *pv2)
{
	return strcmp((*((const JsonPathAnlStats **) pv1))->pathstr,
				  (*((const JsonPathAnlStats **) pv2))->pathstr);
}

/*
 * jsonAnalyzeSortPaths
 *		Reads all stats stored in the hash table and sorts them.
 *
 * XXX It's a bit strange we simply store the result in the context instead
 * of just returning it.
 */
static void
jsonAnalyzeSortPaths(JsonAnalyzeContext *ctx)
{
	HASH_SEQ_STATUS		hseq;
	JsonPathAnlStats   *path;
	int					i = 0;

	ctx->npaths = hash_get_num_entries(ctx->pathshash) + 1;
	ctx->paths = MemoryContextAlloc(ctx->mcxt,
									sizeof(*ctx->paths) * ctx->npaths);

	ctx->paths[i++] = ctx->root;

	hash_seq_init(&hseq, ctx->pathshash);

	while ((path = hash_seq_search(&hseq)))
		if (path->collected != JSPCS_NOT_COLLECTED)
			ctx->paths[i++] = path;

	ctx->npaths = i;

	pg_qsort(ctx->paths, ctx->npaths, sizeof(*ctx->paths),
			 JsonPathStatsCompare);
}

/*
 * jsonAnalyzePaths
 *		Sort the paths and calculate statistics for each of them.
 *
 * Now that we're done with processing the documents, we sort the paths
 * we extracted and calculate stats for each of them.
 *
 * XXX I wonder if we could do this in two phases, to maybe not collect
 * (or even accumulate) values for paths that are not interesting.
 */
static void
jsonAnalyzePaths(JsonAnalyzeContext	*ctx)
{
	int	i;

	jsonAnalyzeSortPaths(ctx);

	for (i = 0; i < ctx->npaths; i++)
		jsonAnalyzePath(ctx, ctx->paths[i]);
}

static Datum
jsonAnalyzeBuildPathStatsHeader(const char *prefix, int prefixlen,
								const char **collectedPaths)
{
	JsonbParseState	   *ps = NULL;
	JsonbValue		   *res;
	JsonbValue			jbv;

	pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

	pushJsonbKey(&ps, &jbv, "prefix");
	JsonValueInitStringWithLen(&jbv,
							   memcpy(palloc(prefixlen), prefix, prefixlen),
							   prefixlen);
	pushJsonbValue(&ps, WJB_VALUE, &jbv);

	pushJsonbKey(&ps, &jbv, "paths");
	pushJsonbValue(&ps, WJB_BEGIN_ARRAY, NULL);
	while (*collectedPaths)
		pushJsonbElemString(&ps, &jbv, *collectedPaths++);
	pushJsonbValue(&ps, WJB_END_ARRAY, NULL);

	res = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);

	return JsonbPGetDatum(JsonbValueToJsonb(res));
}

/*
 * jsonAnalyzeBuildPathStatsArray
 *		???
 */
static Datum *
jsonAnalyzeBuildPathStatsArray(JsonPathAnlStats **paths, int npaths, int *nvals,
								const char *prefix, int prefixlen,
								const char **collectedPaths)
{
	Datum	   *values = palloc(sizeof(Datum) * (npaths + 1));
	int			i;
	int			j = 0;

	values[j++] = jsonAnalyzeBuildPathStatsHeader(prefix, prefixlen,
												  collectedPaths);

	for (i = 0; i < npaths; i++)
		if (paths[i]->collected != JSPCS_NOT_COLLECTED)
			values[j++] = JsonbPGetDatum(paths[i]->stats);

	*nvals = j;

	return values;
}

/*
 * jsonAnalyzeMakeStats
 *		???
 */
static Datum *
jsonAnalyzeMakeStats(JsonAnalyzeContext *ctx, int *numvalues)
{
	Datum		   *values;
	MemoryContext	oldcxt = MemoryContextSwitchTo(ctx->stats->anl_context);

	values = jsonAnalyzeBuildPathStatsArray(ctx->paths, ctx->npaths,
											numvalues, "$", 1,
											ctx->collectedPaths);

	MemoryContextSwitchTo(oldcxt);

	return values;
}

/*
 * jsonAnalyzeBuildSubPathsData
 *		???
 */
bool
jsonAnalyzeBuildSubPathsData(Datum *pathsDatums, int npaths, int index,
							 const char	*path, int pathlen,
							 bool includeSubpaths, float4 nullfrac,
							 Datum *pvals, Datum *pnums,
							 const char **collectedPaths)
{
	JsonPathAnlStats  **pvalues = palloc(sizeof(*pvalues) * npaths);
	Datum			   *values;
	Datum				numbers[1];
	JsonbValue			pathkey;
	int					nsubpaths = 0;
	int					nvalues;
	int					i;

	JsonValueInitStringWithLen(&pathkey, "path", 4);

	for (i = index; i < npaths; i++)
	{
		Jsonb	   *jb = DatumGetJsonbP(pathsDatums[i]);
		JsonbValue *jbv = findJsonbValueFromContainer(&jb->root, JB_FOBJECT,
													  &pathkey);

		if (!jbv || jbv->type != jbvString ||
			jbv->val.string.len < pathlen ||
			memcmp(jbv->val.string.val, path, pathlen))
			break;

		pfree(jbv);

		pvalues[nsubpaths] = palloc(sizeof(**pvalues));
		pvalues[nsubpaths]->stats = jb;
		pvalues[nsubpaths]->collected = JSPCS_COLLECTED;

		nsubpaths++;

		if (!includeSubpaths)
			break;
	}

	if (!nsubpaths)
	{
		pfree(pvalues);
		return false;
	}

	values = jsonAnalyzeBuildPathStatsArray(pvalues, nsubpaths, &nvalues,
											path, pathlen, collectedPaths);
	*pvals = PointerGetDatum(construct_array(values, nvalues, JSONBOID, -1,
											 false, 'i'));

	pfree(pvalues);
	pfree(values);

	numbers[0] = Float4GetDatum(nullfrac);
	*pnums = PointerGetDatum(construct_array(numbers, 1, FLOAT4OID, 4,
											 true /*FLOAT4PASSBYVAL*/, 'i'));

	return true;
}

/*
 * jsonAnalyzeInit
 *		Initialize the analyze context so that we can start adding paths.
 */
static void
jsonInitCollectedPaths(JsonAnalyzeContext *ctx, VacAttrStats *stats)
{
	const char	  **collectedPaths = NULL;
	List		   *newPaths;
	HeapTuple		tuple = SearchSysCache3(STATRELATTINH,
										ObjectIdGetDatum(stats->attr->attrelid),
										Int16GetDatum(stats->attr->attnum),
										BoolGetDatum(false));

	if (HeapTupleIsValid(tuple))
	{
		VariableStatData	vardata;
		JsonStatData		jsdata;

		vardata.statsTuple = tuple;
		vardata.rel = NULL;

		if (jsonStatsInit(&jsdata, &vardata))
			collectedPaths = jsdata.collectedPaths;

		ReleaseSysCache(tuple);
	}

	newPaths = stats->options ? stats->options :
				collectedPaths ? NULL : list_make1("$");

	if (newPaths)
	{
		ListCell	   *lc;
		const char	  **p = collectedPaths;
		int				i = 0;
		Size			size;

		if (collectedPaths)
			while (*p++)
				i++;

		size = sizeof(char *) * (list_length(newPaths) + i + 1);

		collectedPaths = collectedPaths ? repalloc(collectedPaths, size)
										: palloc(size);

		foreach(lc, newPaths)
		{
			const char	   *path = jsonAnalyzeNormalizePath(lfirst(lc));
			int				j;

			for (j = 0; j < i; j++)
				if (!strcmp(collectedPaths[j], path))
					break;

			if (j >= i)
				collectedPaths[i++] = path;
		}

		collectedPaths[i] = NULL;
	}

	ctx->collectedPaths = collectedPaths;
}

static void
jsonAnalyzeInit(JsonAnalyzeContext *ctx, VacAttrStats *stats,
				AnalyzeAttrFetchFunc fetchfunc,
				int samplerows, double totalrows)
{
	HASHCTL			hash_ctl;

	memset(ctx, 0, sizeof(*ctx));

	ctx->stats = stats;
	ctx->fetchfunc = fetchfunc;
	ctx->mcxt = CurrentMemoryContext;
	ctx->samplerows = samplerows;
	ctx->totalrows = totalrows;
	ctx->target = stats->attr->attstattarget;
	ctx->scalarsOnly = false;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(JsonPathEntry);
	hash_ctl.entrysize = sizeof(JsonPathAnlStats);
	hash_ctl.hash = JsonPathHash;
	hash_ctl.match = JsonPathMatch;
	hash_ctl.hcxt = ctx->mcxt;

	ctx->pathshash = hash_create("JSON analyze path table", 100, &hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	ctx->root = MemoryContextAllocZero(ctx->mcxt, sizeof(JsonPathAnlStats));
	ctx->root->pathstr = "$";

	jsonInitCollectedPaths(ctx, stats);

	ctx->root->collected = jsonAnalyzePathIsCollected(ctx->collectedPaths,
													  ctx->root->pathstr,
													  strlen(ctx->root->pathstr));
}

/*
 * jsonAnalyzePass
 *		One analysis pass over the JSON column.
 *
 * Performs one analysis pass on the JSON documents, and passes them to the
 * custom analyzefunc.
 */
static void
jsonAnalyzePass(JsonAnalyzeContext *ctx,
				void (*analyzefunc)(JsonAnalyzeContext *, Jsonb *, void *),
				void *analyzearg)
{
	int	row_num;

	MemoryContext	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
												"Json Analyze Pass Context",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContext	oldcxt = MemoryContextSwitchTo(tmpcxt);

	ctx->null_cnt = 0;
	ctx->analyzed_cnt = 0;
	ctx->total_width = 0;

	/* Loop over the arrays. */
	for (row_num = 0; row_num < ctx->samplerows; row_num++)
	{
		Datum		value;
		Jsonb	   *jb;
		Size		width;
		bool		isnull;

		vacuum_delay_point();

		value = ctx->fetchfunc(ctx->stats, row_num, &isnull);

		if (isnull)
		{
			/* json is null, just count that */
			ctx->null_cnt++;
			continue;
		}

		width = toast_raw_datum_size(value);

		ctx->total_width += VARSIZE_ANY(DatumGetPointer(value)); /* FIXME raw width? */

		/* Skip too-large values. */
#define JSON_WIDTH_THRESHOLD (100 * 1024)

		if (width > JSON_WIDTH_THRESHOLD)
			continue;

		ctx->analyzed_cnt++;

		jb = DatumGetJsonbP(value);

		MemoryContextSwitchTo(oldcxt);

		analyzefunc(ctx, jb, analyzearg);

		oldcxt = MemoryContextSwitchTo(tmpcxt);
		MemoryContextReset(tmpcxt);
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * compute_json_stats() -- compute statistics for a json column
 */
static void
compute_json_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
				   int samplerows, double totalrows)
{
	JsonAnalyzeContext	ctx;

	jsonAnalyzeInit(&ctx, stats, fetchfunc, samplerows, totalrows);

	/* XXX Not sure what the first branch is doing (or supposed to)? */
	if (false)
	{
		jsonAnalyzePass(&ctx, jsonAnalyzeJson, NULL);
		jsonAnalyzePaths(&ctx);
	}
	else
	{
		int				i;
		MemoryContext	oldcxt;
		MemoryContext	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
													"Json Analyze Tmp Context",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

		elog(DEBUG1, "analyzing %s attribute \"%s\"",
			stats->attrtypid == JSONBOID ? "jsonb" : "json",
			NameStr(stats->attr->attname));

		elog(DEBUG1, "collecting json paths");

		oldcxt = MemoryContextSwitchTo(tmpcxt);

		/*
		 * XXX It's not immediately clear why this is (-1) and not simply
		 * NULL. It crashes, so presumably it's used to tweak the behavior,
		 * but it's not clear why/how, and it affects place that is pretty
		 * far away, and so not obvious. We should use some sort of flag
		 * with a descriptive name instead.
		 *
		 * XXX If I understand correctly, we simply collect all paths first,
		 * without accumulating any Values. And then in the next step we
		 * process each path independently, probably to save memory (we
		 * don't want to accumulate all values for all paths, with a lot
		 * of duplicities).
		 */
		jsonAnalyzePass(&ctx, jsonAnalyzeJson, (void *) -1);
		jsonAnalyzeSortPaths(&ctx);

		MemoryContextReset(tmpcxt);

		for (i = 0; i < ctx.npaths; i++)
		{
			JsonPathAnlStats *path = ctx.paths[i];

			if (path->collected == JSPCS_NOT_COLLECTED)
				continue;

			elog(DEBUG1, "analyzing json path (%d/%d) %s",
				 i + 1, ctx.npaths, path->pathstr);

			jsonAnalyzePass(&ctx, jsonAnalyzeJsonPath, path);
			jsonAnalyzePath(&ctx, path);

			MemoryContextReset(tmpcxt);
		}

		MemoryContextSwitchTo(oldcxt);

		MemoryContextDelete(tmpcxt);
	}

	/* We can only compute real stats if we found some non-null values. */
	if (ctx.null_cnt >= samplerows)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		stats->stawidth = 0;		/* "unknown" */
		stats->stadistinct = 0.0;	/* "unknown" */
	}
	else if (!ctx.analyzed_cnt)
	{
		int	nonnull_cnt = samplerows - ctx.null_cnt;

		/* We found some non-null values, but they were all too wide */
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) ctx.null_cnt / (double) samplerows;
		stats->stawidth = ctx.total_width / (double) nonnull_cnt;
		/* Assume all too-wide values are distinct, so it's a unique column */
		stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
	}
	else
	{
		VacAttrStats   *jsstats = &ctx.root->vstats.jsons.stats;
		int				i;
		int				empty_slot = -1;

		stats->stats_valid = true;

		stats->stanullfrac	= jsstats->stanullfrac;
		stats->stawidth		= jsstats->stawidth;
		stats->stadistinct	= jsstats->stadistinct;

		/*
		 * We need to store the statistics the statistics slots. We simply
		 * store the regular stats in the first slots, and then we put the
		 * JSON stats into the first empty slot.
		 */
		for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
		{
			/* once we hit an empty slot, we're done */
			if (!jsstats->staop[i])
			{
				empty_slot = i;		/* remember the empty slot */
				break;
			}

			stats->stakind[i] 		= jsstats->stakind[i];
			stats->staop[i] 		= jsstats->staop[i];
			stats->stanumbers[i] 	= jsstats->stanumbers[i];
			stats->stavalues[i] 	= jsstats->stavalues[i];
			stats->statypid[i] 		= jsstats->statypid[i];
			stats->statyplen[i] 	= jsstats->statyplen[i];
			stats->statypbyval[i] 	= jsstats->statypbyval[i];
			stats->statypalign[i] 	= jsstats->statypalign[i];
			stats->numnumbers[i] 	= jsstats->numnumbers[i];
			stats->numvalues[i] 	= jsstats->numvalues[i];
		}

		Assert((empty_slot >= 0) && (empty_slot < STATISTIC_NUM_SLOTS));

		stats->stakind[empty_slot] = STATISTIC_KIND_JSON;
		stats->staop[empty_slot] = InvalidOid;
		stats->numnumbers[empty_slot] = 1;
		stats->stanumbers[empty_slot] = MemoryContextAlloc(stats->anl_context,
														   sizeof(float4));
		stats->stanumbers[empty_slot][0] = 0.0; /* nullfrac */
		stats->stavalues[empty_slot] =
				jsonAnalyzeMakeStats(&ctx, &stats->numvalues[empty_slot]);

		/* We are storing jsonb values */
		/* XXX Could the parameters be different on other platforms? */
		stats->statypid[empty_slot] = JSONBOID;
		stats->statyplen[empty_slot] = -1;
		stats->statypbyval[empty_slot] = false;
		stats->statypalign[empty_slot] = 'i';
	}
}

/*
 * json_typanalyze -- typanalyze function for jsonb
 */
Datum
jsonb_typanalyze(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);
	Form_pg_attribute attr = stats->attr;

	/* If the attstattarget column is negative, use the default value */
	/* NB: it is okay to scribble on stats->attr since it's a copy */
	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	stats->compute_stats = compute_json_stats;
	/* see comment about the choice of minrows in commands/analyze.c */
	stats->minrows = 300 * attr->attstattarget;

	PG_RETURN_BOOL(true);
}
