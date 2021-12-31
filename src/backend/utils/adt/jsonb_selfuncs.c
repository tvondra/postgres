/*-------------------------------------------------------------------------
 *
 * jsonb_selfuncs.c
 *	  Functions for selectivity estimation of jsonb operators
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonb_selfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/json_selfuncs.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#define DEFAULT_JSON_CONTAINS_SEL	0.001

/*
 * jsonGetField
 *		Given a JSONB document and a key, extract the JSONB value for the key.
 */
static inline Datum
jsonGetField(Datum obj, const char *field)
{
	Jsonb 	   *jb = DatumGetJsonbP(obj);
	JsonbValue *jbv = findJsonbValueFromContainerLen(&jb->root, JB_FOBJECT,
													 field, strlen(field));
	return jbv ? JsonbPGetDatum(JsonbValueToJsonb(jbv)) : PointerGetDatum(NULL);
}

/*
 * jsonGetFloat4
 *		Given a JSONB value, interpret it as a float4 value.
 *
 * This expects the JSONB value to be a numeric, because that's how we store
 * floats in JSONB, and we cast it to float4.
 *
 * XXX Not sure assert is a sufficient protection against different types of
 * JSONB values to be passed in.
 */
static inline Datum
jsonGetFloat4(Datum jsonb)
{
	Jsonb	   *jb = DatumGetJsonbP(jsonb);
	JsonbValue	jv;

	JsonbExtractScalar(&jb->root, &jv);
	Assert(jv.type == jbvNumeric);

	return DirectFunctionCall1(numeric_float4, NumericGetDatum(jv.val.numeric));
}

/*
 * jsonStatsInit
 *		Given a pg_statistic tuple, expand STATISTIC_KIND_JSON into JsonStats.
 */
bool
jsonStatsInit(JsonStats data, const VariableStatData *vardata)
{
	Jsonb	   *jb;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	jbv;

	memset(&data->attslot, 0, sizeof(data->attslot));
	data->statsTuple = vardata->statsTuple;

	/* FIXME Could be before the memset, I guess? Checking vardata->statsTuple. */
	if (!data->statsTuple)
		return false;

	/* Were there just NULL values in the column? No JSON stats, but still useful. */
	if (((Form_pg_statistic) GETSTRUCT(data->statsTuple))->stanullfrac >= 1.0)
	{
		data->nullfrac = 1.0;
		return true;
	}

	/* Do we have the JSON stats built in the pg_statistic? */
	if (!get_attstatsslot(&data->attslot, data->statsTuple,
						  STATISTIC_KIND_JSON, InvalidOid,
						  ATTSTATSSLOT_NUMBERS | ATTSTATSSLOT_VALUES))
		return false;

	/* XXX Not sure what this means / how could it happen? */
	if (data->attslot.nvalues < 2)
	{
		free_attstatsslot(&data->attslot);
		return false;
	}

	/* XXX If the ACL check was not OK, would we even get here? */
	data->acl_ok = vardata->acl_ok;
	data->rel = vardata->rel;
	data->nullfrac =
		data->attslot.nnumbers > 0 ? data->attslot.numbers[0] : 0.0;
	data->prefix = NULL;
	data->collectedPaths = NULL;
	data->values = data->attslot.values;
	data->nvalues = data->attslot.nvalues;

	jb = DatumGetJsonbP(data->values[0]);

	it = JsonbIteratorInit(&jb->root);

	while ((tok = JsonbIteratorNext(&it, &jbv, true)) != WJB_DONE)
	{
		if (tok == WJB_KEY)
		{
			if (jbv.val.string.len == 6 &&
				!strncmp(jbv.val.string.val, "prefix", 6))
			{
				tok = JsonbIteratorNext(&it, &jbv, true);
				Assert(tok == WJB_VALUE && jbv.type == jbvString);
				data->prefix = jbv.val.string.val;
				data->prefixlen = jbv.val.string.len;
			}
			else if (jbv.val.string.len == 5 &&
					 !strncmp(jbv.val.string.val, "paths", 5))
			{
				JsonbContainer	   *jbc;
				JsonbIterator	   *ita;
				const char		  **paths;
				int					npaths;
				int					i = 0;

				tok = JsonbIteratorNext(&it, &jbv, true);
				Assert(tok == WJB_VALUE);
				Assert(jbv.type == jbvBinary);
				jbc = jbv.val.binary.data;
				Assert(JsonContainerIsArray(jbc));

				npaths = JsonContainerSize(jbc);

				paths = palloc(sizeof(*paths) * (npaths + 1));

				ita = JsonbIteratorInit(jbv.val.binary.data);

				while ((tok = JsonbIteratorNext(&ita, &jbv, true)) != WJB_DONE)
				{
					if (tok == WJB_ELEM)
					{
						Assert(jbv.type == jbvString);
						paths[i++] = pnstrdup(jbv.val.string.val,
											  jbv.val.string.len);
					}
				}

				paths[i] = NULL;

				data->collectedPaths = paths;
			}
		}
	}

	return data->collectedPaths && data->prefix;
}

/*
 * jsonStatsRelease
 *		Release resources (statistics slot) associated with the JsonStats value.
 */
void
jsonStatsRelease(JsonStats data)
{
	free_attstatsslot(&data->attslot);
}

/*
 * jsonPathStatsGetSpecialStats
 *		Extract statistics of given type for JSON path.
 *
 * XXX This does not really extract any stats, it merely allocates the struct?
 */
static JsonPathStats
jsonPathStatsGetSpecialStats(JsonPathStats pstats, JsonPathStatsType type)
{
	JsonPathStats stats;

	if (!pstats)
		return NULL;

	stats = palloc(sizeof(*stats));
	*stats = *pstats;
	stats->path = memcpy(palloc(stats->pathlen), stats->path, stats->pathlen);
	stats->type = type;

	return stats;
}

/*
 * jsonPathStatsGetLengthStats
 *		Extract statistics of lengths (for arrays or objects) for the path.
 */
JsonPathStats
jsonPathStatsGetLengthStats(JsonPathStats pstats)
{
	/*
	 * The length statistics is relevant only for values that are objects or
	 * arrays. So if we observed no such values, we know there can't be such
	 * statistics and so we simply return NULL.
	 */
	if (jsonPathStatsGetTypeFreq(pstats, jbvObject, 0.0) <= 0.0 &&
		jsonPathStatsGetTypeFreq(pstats, jbvArray, 0.0) <= 0.0)
		return NULL;

	return jsonPathStatsGetSpecialStats(pstats, JsonPathStatsLength);
}

/*
 * jsonPathStatsGetArrayLengthStats
 *		Extract statistics of lengths for arrays.
 *
 * XXX Why doesn't this do jsonPathStatsGetTypeFreq check similar to what
 * jsonPathStatsGetLengthStats does?
 */
static JsonPathStats
jsonPathStatsGetArrayLengthStats(JsonPathStats pstats)
{
	return jsonPathStatsGetSpecialStats(pstats, JsonPathStatsArrayLength);
}

/*
 * jsonPathStatsCompare
 *		Compare two JsonPathStats structs, so that we can sort them.
 *
 * We do this so that we can search for stats for a given path simply by
 * bsearch().
 *
 * XXX We never build two structs for the same path, so we know the paths
 * are different - one may be a prefix of the other, but then we sort the
 * strings by length.
 */
static int
jsonPathStatsComparePath(JsonbValue const *path1, JsonbValue const *path2)
{
	int			res;

	/* XXX Not sure about this? Does empty path mean global stats? */
	if (!path2 || path2->type != jbvString)
		return 2;

	/* compare the shared part first, then compare by length */
	res = memcmp(path1->val.string.val, path2->val.string.val,
				 Min(path1->val.string.len, path2->val.string.len));

	return res > 0 ? 2 :
		   res < 0 ? -2 :
		   path1->val.string.len > path2->val.string.len ? 1 :
		   path1->val.string.len < path2->val.string.len ? -1 : 0;
}

static JsonbValue *
jsonPathStatsGetPath(Datum datum)
{
	Jsonb	   *jsonb = DatumGetJsonbP(datum);
	JsonbValue	pathkey;

	JsonValueInitStringWithLen(&pathkey, "path", 4);
	return findJsonbValueFromContainer(&jsonb->root, JB_FOBJECT, &pathkey);
}


static int
jsonPathStatsCompare(const void *pv1, const void *pv2)
{
	JsonbValue const   *path1 = pv1;
	Datum const		   *pdatum = pv2;
	JsonbValue		   *path2 = jsonPathStatsGetPath(*pdatum);

	return jsonPathStatsComparePath(path1, path2);
}

/*
 * jsonStatsFindPathStats
 *		Find stats for a given path.
 *
 * The stats are sorted by path, so we can simply do bsearch().
 */
static JsonPathStats
jsonStatsFindPathStats(JsonStats jsdata, char *path, int pathlen)
{
	JsonPathStats stats;
	JsonbValue	jbvkey;
	Datum	   *pdatum;

	if (jsonAnalyzePathIsCollected(jsdata->collectedPaths, path, pathlen) !=
																JSPCS_COLLECTED)
		pdatum = NULL;
	else
	{
		JsonValueInitStringWithLen(&jbvkey, path, pathlen);

		pdatum = bsearch(&jbvkey, jsdata->values + 1, jsdata->nvalues - 1,
						 sizeof(*jsdata->values), jsonPathStatsCompare);

		if (!pdatum)
			return NULL;
	}

	stats = palloc(sizeof(*stats));
	stats->datum = pdatum;
	stats->data = jsdata;
	stats->path = path;
	stats->pathlen = pathlen;
	stats->type = JsonPathStatsValues;

	return stats;
}

/*
 * jsonStatsGetPathStatsStr
 *		???
 *
 * XXX Seems to do essentially what jsonStatsFindPathStats, except that it also
 * considers jsdata->prefix. Seems fairly easy to combine those into a single
 * function.
 */
JsonPathStats
jsonStatsGetPathStatsStr(JsonStats jsdata, const char *subpath, int subpathlen)
{
	JsonPathStats stats;
	char	   *path;
	int			pathlen;

	if (jsdata->nullfrac >= 1.0)
		return NULL;

	pathlen = jsdata->prefixlen + subpathlen - 1;
	path = palloc(pathlen);

	memcpy(path, jsdata->prefix, jsdata->prefixlen);
	memcpy(&path[jsdata->prefixlen], &subpath[1], subpathlen - 1);

	stats = jsonStatsFindPathStats(jsdata, path, pathlen);

	if (!stats)
		pfree(path);

	return stats;
}

/*
 * jsonPathAppendEntry
 *		Append entry (represented as simple string) to a path.
 */
static void
jsonPathAppendEntry(StringInfo path, const char *entry)
{
	appendStringInfoCharMacro(path, '.');
	escape_json(path, entry);
}

/*
 * jsonPathAppendEntryWithLen
 *		Append string (represented as string + length) to a path.
 *
 * XXX Doesn't this need ecape_json too?
 */
static void
jsonPathAppendEntryWithLen(StringInfo path, const char *entry, int len)
{
	char *tmpentry = pnstrdup(entry, len);
	jsonPathAppendEntry(path, tmpentry);
	pfree(tmpentry);
}

/*
 * jsonPathStatsGetSubpath
 *		???
 */
JsonPathStats
jsonPathStatsGetSubpath(JsonPathStats pstats, const char *key, int keylen)
{
	JsonPathStats spstats;
	char	   *path;
	int			pathlen;

	if (key)
	{
		StringInfoData str;

		initStringInfo(&str);
		appendBinaryStringInfo(&str, pstats->path, pstats->pathlen);
		jsonPathAppendEntryWithLen(&str, key, keylen);

		path = str.data;
		pathlen = str.len;
	}
	else
	{
		pathlen = pstats->pathlen + 2;
		path = palloc(pathlen + 1);
		snprintf(path, pstats->pathlen + pathlen, "%.*s.#",
				 pstats->pathlen, pstats->path);
	}

	spstats = jsonStatsFindPathStats(pstats->data, path, pathlen);
	if (!spstats)
		pfree(path);

	return spstats;
}

/*
 * jsonPathStatsGetArrayIndexSelectivity
 *		Given stats for a path, determine selectivity for an array index.
 */
Selectivity
jsonPathStatsGetArrayIndexSelectivity(JsonPathStats pstats, int index)
{
	JsonPathStats lenstats = jsonPathStatsGetArrayLengthStats(pstats);
	JsonbValue	tmpjbv;
	Jsonb	   *jb;

	/*
	 * If we have no array length stats, assume all documents match.
	 *
	 * XXX Shouldn't this use a default smaller than 1.0? What do the selfuncs
	 * for regular arrays use?
	 */
	if (!lenstats)
		return 1.0;

	jb = JsonbValueToJsonb(JsonValueInitInteger(&tmpjbv, index));

	/* calculate fraction of elements smaller than the index */
	return jsonSelectivity(lenstats, JsonbPGetDatum(jb), JsonbGtOperator);
}

/*
 * jsonStatsGetPathStats
 *		???
 *		
 * XXX I guess pathLen stored number of pathEntries elements, so it should be
 * nEntries or something. pathLen implies it's a string length.
 */
static JsonPathStats
jsonStatsGetPathStats(JsonStats jsdata, Datum *pathEntries, int pathLen,
					  float4 *nullfrac)
{
	JsonPathStats pstats;
	Selectivity	sel = 1.0;
	int			i;

	if (!pathEntries && pathLen < 0)
	{
		if ((pstats = jsonStatsGetPathStatsStr(jsdata, "$.#", 3)))
		{
			sel = jsonPathStatsGetArrayIndexSelectivity(pstats, -1 - pathLen);
			sel /= jsonPathStatsGetFreq(pstats, 0.0);
		}
	}
	else
	{
		pstats = jsonStatsGetPathStatsStr(jsdata, "$", 1);

		for (i = 0; pstats && i < pathLen; i++)
		{
			char	   *key = text_to_cstring(DatumGetTextP(pathEntries[i]));
			int			keylen = strlen(key);

			/* XXX What's this key "0123456789" about? */
			if (key[0] >= '0' && key[0] <= '9' &&
				key[strspn(key, "0123456789")] == '\0')
			{
				char	   *tail;
				long		index;

				errno = 0;
				index = strtol(key, &tail, 10);

				if (*tail || errno || index > INT_MAX || index < 0)
					pstats = jsonPathStatsGetSubpath(pstats, key, keylen);
				else
				{
					float4	arrfreq;

					/* FIXME consider key also */
					pstats = jsonPathStatsGetSubpath(pstats, NULL, 0);
					sel *= jsonPathStatsGetArrayIndexSelectivity(pstats, index);
					arrfreq = jsonPathStatsGetFreq(pstats, 0.0);

					if (arrfreq > 0.0)
						sel /= arrfreq;
				}
			}
			else
				pstats = jsonPathStatsGetSubpath(pstats, key, keylen);

			pfree(key);
		}
	}

	*nullfrac = 1.0 - sel;

	return pstats;
}

/*
 * jsonPathStatsGetNextKeyStats
 *		???
 */
bool
jsonPathStatsGetNextKeyStats(JsonPathStats stats, JsonPathStats *pkeystats,
							 bool keysOnly)
{
	JsonbValue	jbvkey;
	JsonPathStats keystats = *pkeystats;
	int			index = !keystats && !stats->datum ? 1 :
		(keystats ? keystats : stats)->datum - stats->data->values + 1;

	JsonValueInitStringWithLen(&jbvkey, stats->path, stats->pathlen);

	for (; index < stats->data->nvalues; index++)
	{
		JsonbValue *jbvpath = jsonPathStatsGetPath(stats->data->values[index]);
		int			cmp = jsonPathStatsComparePath(&jbvkey, jbvpath);

		if (cmp >= 0)
			continue;

		if (cmp < -1)
			break;

		if (keysOnly)
		{
			const char *c = &jbvpath->val.string.val[stats->pathlen];

			Assert(*c == '.');
			c++;

			if (*c == '#')
			{
				if (keysOnly || jbvpath->val.string.len > stats->pathlen + 2)
					continue;
			}
			else
			{
				Assert(*c == '"');

				while (*++c != '"')
					if (*c == '\\')
						c++;

				if (c - jbvpath->val.string.val < jbvpath->val.string.len - 1)
					continue;
			}
		}

		if (!keystats)
			keystats = palloc(sizeof(*keystats));

		keystats->datum = &stats->data->values[index];
		keystats->data = stats->data;
		keystats->pathlen = jbvpath->val.string.len;
		keystats->path = memcpy(palloc(keystats->pathlen),
								jbvpath->val.string.val, keystats->pathlen);
		keystats->type = JsonPathStatsValues;

		*pkeystats = keystats;

		return true;
	}

	return false;
}

/*
 * jsonStatsConvertArray
 *		Convert a JSONB array into an array of some regular data type.
 *
 * The "type" identifies what elements are in the input JSONB array, while
 * typid determines the target type.
 */
static Datum
jsonStatsConvertArray(Datum jsonbValueArray, JsonStatType type, Oid typid,
					  float4 multiplier)
{
	Datum	   *values;
	Jsonb	   *jbvals;
	JsonbValue	jbv;
	JsonbIterator *it;
	JsonbIteratorToken r;
	int			nvalues;
	int			i;

	if (!DatumGetPointer(jsonbValueArray))
		return PointerGetDatum(NULL);

	jbvals = DatumGetJsonbP(jsonbValueArray);

	nvalues = JsonContainerSize(&jbvals->root);

	values = palloc(sizeof(Datum) * nvalues);

	for (i = 0, it = JsonbIteratorInit(&jbvals->root);
		(r = JsonbIteratorNext(&it, &jbv, true)) != WJB_DONE;)
	{
		if (r == WJB_ELEM)
		{
			Datum value;

			switch (type)
			{
				case JsonStatJsonb:
				case JsonStatJsonbWithoutSubpaths:
					value = JsonbPGetDatum(JsonbValueToJsonb(&jbv));
					break;

				case JsonStatText:
				case JsonStatString:
					Assert(jbv.type == jbvString);
					value = PointerGetDatum(
								cstring_to_text_with_len(jbv.val.string.val,
														 jbv.val.string.len));
					break;

				case JsonStatNumeric:
					Assert(jbv.type == jbvNumeric);
					value = DirectFunctionCall1(numeric_float4,
												NumericGetDatum(jbv.val.numeric));
					value = Float4GetDatum(DatumGetFloat4(value) * multiplier);
					break;

				default:
					elog(ERROR, "invalid json stat type %d", type);
					value = (Datum) 0;
					break;
			}

			Assert(i < nvalues);
			values[i++] = value;
		}
	}

	Assert(i == nvalues);

	/*
	 * FIXME Does this actually work on all 32/64-bit systems? What if typid is
	 * FLOAT8OID or something? Should look at TypeCache instead, probably.
	 */
	return PointerGetDatum(
			construct_array(values, nvalues,
							typid,
							typid == FLOAT4OID ? 4 : -1,
							typid == FLOAT4OID ? true /* FLOAT4PASSBYVAL */ : false,
							'i'));
}

/*
 * jsonPathStatsExtractData
 *		Extract pg_statistics values from statistics for a single path.
 *
 *
 */
static bool
jsonPathStatsExtractData(JsonPathStats pstats, JsonStatType stattype,
						 float4 nullfrac, StatsData *statdata)
{
	Datum		data;
	Datum		nullf;
	Datum		dist;
	Datum		width;
	Datum		mcv;
	Datum		hst;
	Datum		corr;
	Oid			type;
	Oid			eqop;
	Oid			ltop;
	const char *key;
	StatsSlot  *slot = statdata->slots;

	nullfrac = 1.0 - (1.0 - pstats->data->nullfrac) * (1.0 - nullfrac);

	switch (stattype)
	{
		case JsonStatJsonb:
		case JsonStatJsonbWithoutSubpaths:
			key = pstats->type == JsonPathStatsArrayLength ? "array_length" :
				  pstats->type == JsonPathStatsLength ? "length" : "json";
			type = JSONBOID;
			eqop = JsonbEqOperator;
			ltop = JsonbLtOperator;
			break;
		case JsonStatText:
			key = "text";
			type = TEXTOID;
			eqop = TextEqualOperator;
			ltop = TextLessOperator;
			break;
		case JsonStatString:
			key = "string";
			type = TEXTOID;
			eqop = TextEqualOperator;
			ltop = TextLessOperator;
			break;
		case JsonStatNumeric:
			key = "numeric";
			type = NUMERICOID;
			eqop = NumericEqOperator;
			ltop = NumericLtOperator;
			break;
		default:
			elog(ERROR, "invalid json statistic type %d", stattype);
			break;
	}

	Assert(pstats->datum);

	data = jsonGetField(*pstats->datum, key);
	if (!data)
		return false;

	nullf = jsonGetField(data, "nullfrac");
	dist = jsonGetField(data, "distinct");
	width = jsonGetField(data, "width");
	mcv = jsonGetField(data, "mcv");
	hst = jsonGetField(data, "histogram");
	corr = jsonGetField(data, "correlation");

	statdata->nullfrac = DatumGetPointer(nullf) ?
							DatumGetFloat4(jsonGetFloat4(nullf)) : 0.0;
	statdata->distinct = DatumGetPointer(dist) ?
							DatumGetFloat4(jsonGetFloat4(dist)) : 0.0;
	statdata->width = DatumGetPointer(width) ?
							(int32) DatumGetFloat4(jsonGetFloat4(width)) : 0;

	statdata->nullfrac += (1.0 - statdata->nullfrac) * nullfrac;

	if (DatumGetPointer(mcv))
	{
		slot->kind = STATISTIC_KIND_MCV;
		slot->opid = eqop;
		slot->numbers = jsonStatsConvertArray(jsonGetField(mcv, "numbers"),
											  JsonStatNumeric, FLOAT4OID,
											  1.0 - nullfrac);
		slot->values  = jsonStatsConvertArray(jsonGetField(mcv, "values"),
											  stattype, type, 0);
		slot++;
	}

	if (DatumGetPointer(hst))
	{
		slot->kind = STATISTIC_KIND_HISTOGRAM;
		slot->opid = ltop;
		slot->numbers = jsonStatsConvertArray(jsonGetField(hst, "numbers"),
											  JsonStatNumeric, FLOAT4OID, 1.0);
		slot->values  = jsonStatsConvertArray(jsonGetField(hst, "values"),
											  stattype, type, 0);
		slot++;
	}

	if (DatumGetPointer(corr))
	{
		Datum	correlation = jsonGetFloat4(corr);
		slot->kind = STATISTIC_KIND_CORRELATION;
		slot->opid = ltop;
		slot->numbers = PointerGetDatum(construct_array(&correlation, 1,
														FLOAT4OID, 4, true,
														'i'));
		slot++;
	}

	if ((stattype == JsonStatJsonb ||
		 stattype == JsonStatJsonbWithoutSubpaths) &&
		jsonAnalyzeBuildSubPathsData(pstats->data->values,
									 pstats->data->nvalues,
									 pstats->datum - pstats->data->values,
									 pstats->path,
									 pstats->pathlen,
									 stattype == JsonStatJsonb,
									 nullfrac,
									 &slot->values,
									 &slot->numbers,
									 pstats->data->collectedPaths))
	{
		slot->kind = STATISTIC_KIND_JSON;
		slot++;
	}

	return true;
}

static float4
jsonPathStatsGetFloat(JsonPathStats pstats, const char *key,
					  float4 defaultval)
{
	Datum		freq;

	if (!pstats || !pstats->datum ||
		!(freq = jsonGetField(*pstats->datum, key)))
		return defaultval;

	return DatumGetFloat4(jsonGetFloat4(freq));
}

float4
jsonPathStatsGetFreq(JsonPathStats pstats, float4 defaultfreq)
{
	return jsonPathStatsGetFloat(pstats, "freq", defaultfreq);
}

float4
jsonPathStatsGetAvgArraySize(JsonPathStats pstats)
{
	return jsonPathStatsGetFloat(pstats, "avg_array_length", 1.0);
}

/*
 * jsonPathStatsGetTypeFreq
 *		Get frequency of different JSON object types for a given path.
 *
 * JSON documents don't have any particular schema, and the same path may point
 * to values with different types in multiple documents. Consider for example
 * two documents {"a" : "b"} and {"a" : 100} which have both a string and int
 * for the same path. So we track the frequency of different JSON types for
 * each path, so that we can consider this later.
 */
float4
jsonPathStatsGetTypeFreq(JsonPathStats pstats, JsonbValueType type,
						 float4 defaultfreq)
{
	const char *key;

	if (!pstats)
		return defaultfreq;

	/*
	 * When dealing with (object/array) length stats, we only really care about
	 * objects and arrays.
	 */
	if (pstats->type == JsonPathStatsLength ||
		pstats->type == JsonPathStatsArrayLength)
	{
		/* XXX Seems more like an error, no? Why ignore it? */
		if (type != jbvNumeric)
			return 0.0;

		/* FIXME This is really hard to read/understand, with two nested ternary operators. */
		return pstats->type == JsonPathStatsArrayLength
				? jsonPathStatsGetFreq(pstats, defaultfreq)
				: jsonPathStatsGetFloat(pstats, "freq_array", defaultfreq) +
				  jsonPathStatsGetFloat(pstats, "freq_object", defaultfreq);
	}

	/* Which JSON type are we interested in? Pick the right freq_type key. */
	switch (type)
	{
		case jbvNull:
			key = "freq_null";
			break;
		case jbvString:
			key = "freq_string";
			break;
		case jbvNumeric:
			key = "freq_numeric";
			break;
		case jbvBool:
			key = "freq_boolean";
			break;
		case jbvObject:
			key = "freq_object";
			break;
		case jbvArray:
			key = "freq_array";
			break;
		default:
			elog(ERROR, "Invalid jsonb value type: %d", type);
			break;
	}

	return jsonPathStatsGetFloat(pstats, key, defaultfreq);
}

/*
 * jsonPathStatsFormTuple
 *		For a pg_statistic tuple representing JSON statistics.
 *
 * XXX Maybe it's a bit expensive to first build StatsData and then transform it
 * again while building the tuple. Could it be done in a single step? Would it be
 * more efficient? Not sure how expensive it actually is, though.
 */
static HeapTuple
jsonPathStatsFormTuple(JsonPathStats pstats, JsonStatType type, float4 nullfrac)
{
	StatsData	statdata;

	if (!pstats)
		return stats_form_tuple(NULL); /* no such path, return all-NULL stats */

	if (!pstats->datum)
		return NULL; /* stats for this path is not collected */

	/* FIXME What does this mean? */
	if (pstats->pathlen == 1 && pstats->path[0] == '$' &&
		pstats->type == JsonPathStatsValues)
		return heap_copytuple(pstats->data->statsTuple);

	MemSet(&statdata, 0, sizeof(statdata));

	if (!jsonPathStatsExtractData(pstats, type, nullfrac, &statdata))
		return NULL;

	return stats_form_tuple(&statdata);
}

/*
 * jsonStatsGetPathStatsTuple
 *		???
 */
static HeapTuple
jsonStatsGetPathStatsTuple(JsonStats jsdata, JsonStatType type,
						   Datum *path, int pathlen)
{
	float4			nullfrac;
	JsonPathStats	pstats = jsonStatsGetPathStats(jsdata, path, pathlen,
												   &nullfrac);

	return jsonPathStatsFormTuple(pstats, type, nullfrac);
}

/*
 * jsonStatsGetPathFreq
 *		Return frequency of a path (fraction of documents containing it).
 */
static float4
jsonStatsGetPathFreq(JsonStats jsdata, Datum *path, int pathlen)
{
	float4			nullfrac;
	JsonPathStats	pstats = jsonStatsGetPathStats(jsdata, path, pathlen,
												   &nullfrac);
	float4			freq = (1.0 - nullfrac) * jsonPathStatsGetFreq(pstats, 0.0);

	CLAMP_PROBABILITY(freq);
	return freq;
}

/*
 * jsonbStatsVarOpConst
 *		Prepare optimizer statistics for a given operator, from JSON stats.
 *
 * This handles only OpExpr expressions, with variable and a constant. We get
 * the constant as is, and the variable is represented by statistics fetched
 * by get_restriction_variable().
 *
 * opid    - OID of the operator (input parameter)
 * resdata - pointer to calculated statistics for result of operator
 * vardata - statistics for the restriction variable
 * cnst    - constant from the operator expression
 *
 * Returns true when useful optimizer statistics have been calculated.
 */
static bool
jsonbStatsVarOpConst(Oid opid, VariableStatData *resdata,
					 const VariableStatData *vardata, Const *cnst)
{
	JsonStatData jsdata;
	JsonStatType statype = JsonStatJsonb;

	if (!jsonStatsInit(&jsdata, vardata))
		return false;

	switch (opid)
	{
		case JsonbObjectFieldTextOperator:
			statype = JsonStatText;
			/* fall through */
		case JsonbObjectFieldOperator:
		{
			if (cnst->consttype != TEXTOID)
			{
				jsonStatsRelease(&jsdata);
				return false;
			}

			resdata->statsTuple =
				jsonStatsGetPathStatsTuple(&jsdata, statype,
										  &cnst->constvalue, 1);
			break;
		}

		case JsonbArrayElementTextOperator:
			statype = JsonStatText;
			/* fall through */
		case JsonbArrayElementOperator:
		{
			if (cnst->consttype != INT4OID)
			{
				jsonStatsRelease(&jsdata);
				return false;
			}

			resdata->statsTuple =
				jsonStatsGetPathStatsTuple(&jsdata, statype, NULL,
										   -1 - DatumGetInt32(cnst->constvalue));
			break;
		}

		case JsonbExtractPathTextOperator:
			statype = JsonStatText;
			/* fall through */
		case JsonbExtractPathOperator:
		{
			Datum	   *path;
			bool	   *nulls;
			int			pathlen;
			int			i;

			if (cnst->consttype != TEXTARRAYOID)
			{
				jsonStatsRelease(&jsdata);
				return false;
			}

			deconstruct_array(DatumGetArrayTypeP(cnst->constvalue), TEXTOID,
							  -1, false, 'i', &path, &nulls, &pathlen);

			for (i = 0; i < pathlen; i++)
			{
				if (nulls[i])
				{
					pfree(path);
					pfree(nulls);
					PG_RETURN_VOID();
				}
			}

			resdata->statsTuple =
				jsonStatsGetPathStatsTuple(&jsdata, statype, path, pathlen);

			pfree(path);
			pfree(nulls);
			break;
		}

		default:
			jsonStatsRelease(&jsdata);
			return false;
	}

	if (!resdata->statsTuple)
		return false;

	resdata->acl_ok = vardata->acl_ok;
	resdata->freefunc = heap_freetuple;
	Assert(resdata->rel == vardata->rel);
	Assert(resdata->atttype ==
		(statype == JsonStatJsonb ? JSONBOID :
		 statype == JsonStatText ? TEXTOID :
		 /* statype == JsonStatFreq */ BOOLOID));

	jsonStatsRelease(&jsdata);
	return true;
}

/*
 * jsonb_stats
 *		Statistics estimation procedure for JSONB data type.
 *
 * This only supports OpExpr expressions, with (Var op Const) shape.
 *
 * XXX It might be useful to allow recursion, i.e. get_restriction_variable
 * might derive statistics too. I don't think it does that now, right?
 */
Datum
jsonb_stats(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	OpExpr	   *opexpr = (OpExpr *) PG_GETARG_POINTER(1);
	int			varRelid = PG_GETARG_INT32(2);
	VariableStatData *resdata	= (VariableStatData *) PG_GETARG_POINTER(3);
	VariableStatData vardata;
	Node	   *constexpr;
	bool		result;
	bool		varonleft;

	/* should only be called for OpExpr expressions */
	Assert(IsA(opexpr, OpExpr));

	/* Is the expression simple enough? (Var op Const) or similar? */
	if (!get_restriction_variable(root, opexpr->args, varRelid,
								  &vardata, &constexpr, &varonleft))
		return false;

	/* XXX Could we also get varonleft=false in useful cases? */
	result = IsA(constexpr, Const) && varonleft &&
		jsonbStatsVarOpConst(opexpr->opno, resdata, &vardata,
							 (Const *) constexpr);

	ReleaseVariableStats(vardata);

	return result;
}

/*
 * jsonSelectivity
 *		Use JSON statistics to estimate selectivity for (in)equalities.
 *
 * The statistics is represented as (arrays of) JSON values etc. so we
 * need to pass the right operators to the functions.
 */
Selectivity
jsonSelectivity(JsonPathStats stats, Datum scalar, Oid operator)
{
	VariableStatData vardata;
	Selectivity sel;

	if (!stats)
		return 0.0;

	vardata.atttype = JSONBOID;
	vardata.atttypmod = -1;
	vardata.isunique = false;
	vardata.rel = stats->data->rel;
	vardata.var = NULL;
	vardata.vartype = JSONBOID;
	vardata.acl_ok = stats->data->acl_ok;
	vardata.statsTuple = jsonPathStatsFormTuple(stats,
												JsonStatJsonbWithoutSubpaths, 0.0);

	if (operator == JsonbEqOperator)
		sel = var_eq_const(&vardata, operator, InvalidOid, scalar, false, true, false);
	else
		sel = scalarineqsel(NULL, operator,
							operator == JsonbGtOperator ||
							operator == JsonbGeOperator,
							operator == JsonbLeOperator ||
							operator == JsonbGeOperator,
							InvalidOid,
							&vardata, scalar, JSONBOID);

	if (vardata.statsTuple)
		heap_freetuple(vardata.statsTuple);

	return sel;
}

/*
 * jsonSelectivityContains
 *		Estimate selectivity for containment operator on JSON.
 *
 * XXX This really needs more comments explaining the logic.
 */
static Selectivity
jsonSelectivityContains(JsonStats stats, Jsonb *jb)
{
	JsonbValue		v;
	JsonbIterator  *it;
	JsonbIteratorToken r;
	StringInfoData	pathstr;
	struct Path
	{
		struct Path *parent;
		int			len;
		JsonPathStats stats;
		Selectivity	freq;
		Selectivity	sel;
	}			root,
			   *path = &root;
	Selectivity	scalarSel = 0.0;
	Selectivity	sel;
	bool		rawScalar = false;

	initStringInfo(&pathstr);

	appendStringInfo(&pathstr, "$");

	root.parent = NULL;
	root.len = pathstr.len;
	root.stats = jsonStatsGetPathStatsStr(stats, pathstr.data, pathstr.len);
	root.freq = jsonPathStatsGetFreq(root.stats, 0.0);
	root.sel = 1.0;

	if (root.freq <= 0.0)
		return 0.0;

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (r)
		{
			case WJB_BEGIN_OBJECT:
			{
				struct Path *p = palloc(sizeof(*p));

				p->len = pathstr.len;
				p->parent = path;
				p->stats = NULL;
				p->freq = jsonPathStatsGetTypeFreq(path->stats, jbvObject, 0.0);
				if (p->freq <= 0.0)
					return 0.0;
				p->sel = 1.0;
				path = p;
				break;
			}

			case WJB_BEGIN_ARRAY:
			{
				struct Path *p = palloc(sizeof(*p));

				rawScalar = v.val.array.rawScalar;

				appendStringInfo(&pathstr, ".#");
				p->len = pathstr.len;
				p->parent = path;
				p->stats = jsonStatsGetPathStatsStr(stats, pathstr.data,
													pathstr.len);
				p->freq = jsonPathStatsGetFreq(p->stats, 0.0);
				if (p->freq <= 0.0 && !rawScalar)
					return 0.0;
				p->sel = 1.0;
				path = p;

				break;
			}

			case WJB_END_OBJECT:
			case WJB_END_ARRAY:
			{
				struct Path *p = path;

				path = path->parent;
				sel = p->sel * p->freq / path->freq;
				pfree(p);
				pathstr.data[pathstr.len = path->len] = '\0';
				if (pathstr.data[pathstr.len - 1] == '#')
					sel = 1.0 - pow(1.0 - sel,
								jsonPathStatsGetAvgArraySize(path->stats));
				path->sel *= sel;
				break;
			}

			case WJB_KEY:
			{
				pathstr.data[pathstr.len = path->parent->len] = '\0';
				jsonPathAppendEntryWithLen(&pathstr, v.val.string.val,
										   v.val.string.len);
				path->len = pathstr.len;
				break;
			}

			case WJB_VALUE:
			case WJB_ELEM:
			{
				JsonPathStats	pstats = r == WJB_ELEM ? path->stats :
					jsonStatsGetPathStatsStr(stats, pathstr.data, pathstr.len);
				Datum			scalar = JsonbPGetDatum(JsonbValueToJsonb(&v));

				if (path->freq <= 0.0)
					sel = 0.0;
				else
				{
					sel = jsonSelectivity(pstats, scalar, JsonbEqOperator);
					sel /= path->freq;
					if (pathstr.data[pathstr.len - 1] == '#')
						sel = 1.0 - pow(1.0 - sel,
										jsonPathStatsGetAvgArraySize(path->stats));
				}

				path->sel *= sel;

				if (r == WJB_ELEM && path->parent == &root && rawScalar)
					scalarSel = jsonSelectivity(root.stats, scalar,
												JsonbEqOperator);
				break;
			}

			default:
				break;
		}
	}

	sel = scalarSel + root.sel * root.freq;
	CLAMP_PROBABILITY(sel);
	return sel;
}

/*
 * jsonSelectivityExists
 *		Estimate selectivity for JSON "exists" operator.
 */
static Selectivity
jsonSelectivityExists(JsonStats stats, Datum key)
{
	JsonPathStats arrstats;
	JsonbValue	jbvkey;
	Datum		jbkey;
	Selectivity keysel;
	Selectivity scalarsel;
	Selectivity arraysel;
	Selectivity sel;

	JsonValueInitStringWithLen(&jbvkey,
							   VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	jbkey = JsonbPGetDatum(JsonbValueToJsonb(&jbvkey));

	keysel = jsonStatsGetPathFreq(stats, &key, 1);

	scalarsel = jsonSelectivity(jsonStatsGetPathStatsStr(stats, "$", 1),
								jbkey, JsonbEqOperator);

	arrstats = jsonStatsGetPathStatsStr(stats, "$.#", 3);
	arraysel = jsonSelectivity(arrstats, jbkey, JsonbEqOperator);
	arraysel = 1.0 - pow(1.0 - arraysel,
						 jsonPathStatsGetAvgArraySize(arrstats));

	sel = keysel + scalarsel + arraysel;
	CLAMP_PROBABILITY(sel);
	return sel;
}

/*
 * jsonb_sel
 *		The main procedure estimating selectivity for all JSONB operators.
 */
Datum
jsonb_sel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Oid			operator = PG_GETARG_OID(1);
	List	   *args = (List *) PG_GETARG_POINTER(2);
	int			varRelid = PG_GETARG_INT32(3);
	double		sel = DEFAULT_JSON_CONTAINS_SEL;
	Node	   *other;
	Const	   *cnst;
	bool		varonleft;
	JsonStatData stats;
	VariableStatData vardata;

	if (!get_restriction_variable(root, args, varRelid,
								  &vardata, &other, &varonleft))
		PG_RETURN_FLOAT8(sel);

	if (!IsA(other, Const))
		goto out;

	cnst = (Const *) other;

	if (cnst->constisnull)
	{
		sel = 0.0;
		goto out;
	}

	if (!jsonStatsInit(&stats, &vardata))
		goto out;

	switch (operator)
	{
		case JsonbExistsOperator:
			if (!varonleft || cnst->consttype != TEXTOID)
				goto out;

			sel = jsonSelectivityExists(&stats, cnst->constvalue);
			break;

		case JsonbExistsAnyOperator:
		case JsonbExistsAllOperator:
		{
			Datum	   *keys;
			bool	   *nulls;
			Selectivity	freq = 1.0;
			int			nkeys;
			int			i;
			bool		all = operator == JsonbExistsAllOperator;

			if (!varonleft || cnst->consttype != TEXTARRAYOID)
				goto out;

			deconstruct_array(DatumGetArrayTypeP(cnst->constvalue), TEXTOID,
							  -1, false, 'i', &keys, &nulls, &nkeys);

			for (i = 0; i < nkeys; i++)
				if (!nulls[i])
				{
					Selectivity pathfreq = jsonSelectivityExists(&stats,
																 keys[i]);
					freq *= all ? pathfreq : (1.0 - pathfreq);
				}

			pfree(keys);
			pfree(nulls);

			if (!all)
				freq = 1.0 - freq;

			sel = freq;
			break;
		}

		case JsonbContainedOperator:
			/* TODO */
			break;

		case JsonbContainsOperator:
		{
			if (cnst->consttype != JSONBOID)
				goto out;

			sel = jsonSelectivityContains(&stats,
										  DatumGetJsonbP(cnst->constvalue));
			break;
		}
	}

out:
	jsonStatsRelease(&stats);
	ReleaseVariableStats(vardata);

	PG_RETURN_FLOAT8((float8) sel);
}
