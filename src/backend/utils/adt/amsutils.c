/*-------------------------------------------------------------------------
 *
 * amsutils.c
 *	  This file contains support routines required for AMS sketches.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/amsutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/ams.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/memutils.h"


static uint32 murmur3_32(const uint8 * key, int len, uint32 seed);

AMSSketch *
AMSSketchInit(int nrows, int ncounters)
{
	int			len;
	AMSSketch  *sketch;

	len = offsetof(AMSSketch, counters) + nrows * ncounters * sizeof(int);

	sketch = palloc0(len);

	sketch->nrows = nrows;
	sketch->ncounters = ncounters;

	SET_VARSIZE(sketch, len);

	return sketch;
}

void
AMSSketchAddValue(AMSSketch *sketch, const void *val, int len)
{
	int i;

	for (i = 0; i < sketch->nrows; i++)
	{
		/* for each row, compute the column index */
		int j = murmur3_32(val, len, i) % sketch->ncounters;

		/* also compute the {-1,1} function, but use different seeds */
		int d = murmur3_32(val, len, sketch->nrows + i) % 2;

		if (d == 0)
			d = -1;

		/* add +1/-1 to the c-th counter in each row */
		sketch->counters[i * sketch->ncounters + j] += d;
	}

	sketch->count += 1;
}

static uint32
murmur3_32(const uint8 * key, int len, uint32 seed)
{
	uint32 h = seed;

	if (len > 3)
	{
		const uint32* key_x4 = (const uint32*) key;
		int i = len >> 2;

		do {
			uint32 k = *key_x4++;
			k *= 0xcc9e2d51;
			k = (k << 15) | (k >> 17);
			k *= 0x1b873593;
			h ^= k;
			h = (h << 13) | (h >> 19);
			h = (h * 5) + 0xe6546b64;
		} while (--i);

		key = (const uint8*) key_x4;
	}

	if (len & 3)
	{
		int i = len & 3;
		uint32 k = 0;
		key = &key[i - 1];

		do {
			k <<= 8;
			k |= *key--;
		} while (--i);

		k *= 0xcc9e2d51;
		k = (k << 15) | (k >> 17);
		k *= 0x1b873593;
		h ^= k;
	}

	h ^= len;
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;
}
