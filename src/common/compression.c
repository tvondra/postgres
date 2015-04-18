#include "postgres.h"
#include "common/compression.h"

#if HAVE_LIBLZ4
#include "lz4.h"
#include "lz4hc.h"
#endif

#if HAVE_LIBLZO2
#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>
#endif

#if HAVE_LIBSNAPPY
#include "snappy-c.h"
#endif

/* by default use the original pglz compression algorithm */
int compression_algorithm = COMPRESSION_PGLZ;


#if HAVE_LIBLZ4
static int32
lz4_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
static int32
lz4_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize);
static int32
lz4hc_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
static int32
lz4hc_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize);
#endif

#if HAVE_LIBLZO2
static int32
lzo_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
static int32
lzo_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize);
#endif

#if HAVE_LIBSNAPPY
static int32
snappy_compress_pg(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);
static int32
snappy_decompress_pg(const char *source, int32 slen, char *dest,
				int32 rawsize);
#endif

int32
pg_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
		strategy = PGLZ_strategy_default;

	/*
	 * Logic shared by all the algorithms.
	 */
	if (strategy->match_size_good <= 0 ||
		slen < strategy->min_input_size ||
		slen > strategy->max_input_size)
		return -1;

	if (compression_algorithm == COMPRESSION_PGLZ)
		return pglz_compress(source, slen, dest, strategy);

	else if (compression_algorithm == COMPRESSION_NONE)
		/* return (-1) which means 'incompressible' */
		return -1;

#if HAVE_LIBLZ4
	else if (compression_algorithm == COMPRESSION_LZ4)
		return lz4_compress(source, slen, dest, strategy);

	else if (compression_algorithm == COMPRESSION_LZ4HC)
		return lz4hc_compress(source, slen, dest, strategy);
#endif

#if HAVE_LIBLZO2
	else if (compression_algorithm == COMPRESSION_LZO)
		return lzo_compress(source, slen, dest, strategy);
#endif

#if HAVE_LIBSNAPPY
	else if (compression_algorithm == COMPRESSION_SNAPPY)
		return snappy_compress_pg(source, slen, dest, strategy);
#endif

	return -1;
}

int32
pg_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	if (compression_algorithm == COMPRESSION_PGLZ)
		return pglz_decompress(source, slen, dest, rawsize);

	else if (compression_algorithm == COMPRESSION_NONE)
		return -1;

#if HAVE_LIBLZ4
	else if (compression_algorithm == COMPRESSION_LZ4)
		return lz4_decompress(source, slen, dest, rawsize);

	else if (compression_algorithm == COMPRESSION_LZ4HC)
		return lz4hc_decompress(source, slen, dest, rawsize);
#endif

#if HAVE_LIBLZO2
	else if (compression_algorithm == COMPRESSION_LZO)
		return lzo_decompress(source, slen, dest, rawsize);
#endif

#if HAVE_LIBSNAPPY
	else if (compression_algorithm == COMPRESSION_SNAPPY)
		return snappy_decompress_pg(source, slen, dest, rawsize);
#endif

	return -1;
}

#if HAVE_LIBLZ4
static int32
lz4_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	int32 ret;
	int32 result_max;
	int32 need_rate = strategy->min_comp_rate;

	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	result_max = (slen / 100) * (100 - need_rate);

	ret = LZ4_compress_limitedOutput(source, dest, slen, result_max);

	/* LZ4 uses 0 to signal error, we expect -1 in that case. */
	if (ret == 0)
		return -1;

	return ret;
}

static int32
lz4_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	int32 ret = LZ4_decompress_safe (source, dest, slen, rawsize);

	if (ret < 0)
		return -1;

	return ret;
}

/* first a pass of regular LZ4, then LZ4HC (mostly just an experiment) */

static int32
lz4hc_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	int32 ret;
	int32 result_max;
	int32 need_rate = strategy->min_comp_rate;

	char * tmp = palloc(slen);

	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	result_max = (slen / 100) * (100 - need_rate);

	ret = LZ4_compress_limitedOutput(source, tmp, slen, result_max);

	/* LZ4 uses 0 to signal error, we expect -1 in that case. */
	if (ret == 0)
	{
		pfree(tmp);
		return -1;
	}

	/*
	 * this has the unfortunate consequence that if the first pass
	 * is very efficient, but the second one does not reduce the size
	 * sufficiently (and just gets us over result_max) we don't
	 * compress the data at all
	 */

	ret = LZ4_compressHC_limitedOutput(tmp, dest, ret, result_max);

	if (ret == 0)
		ret = -1;

	pfree(tmp);

	return ret;
}

static int32
lz4hc_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	int32 ret;
	char * tmp = palloc(rawsize);

	ret = LZ4_decompress_safe (source, tmp, slen, rawsize);

	if (ret < 0)
	{
		pfree(tmp);
		return -1;
	}

	ret = LZ4_decompress_safe (tmp, dest, ret, rawsize);

	if (ret < 0)
		ret = -1;

	pfree(tmp);
	return ret;
}
#endif


#if HAVE_LIBLZO2
static int32
lzo_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	int r;
	lzo_voidp wrkmem;
	lzo_uint destlen;

	if (lzo_init() != LZO_E_OK)
		return -1;

	/* LZO work memory */
	wrkmem = (lzo_voidp) palloc(LZO1X_1_MEM_COMPRESS);

	r = lzo1x_1_compress((lzo_bytep)source, (lzo_uint)slen,
						 (lzo_bytep)dest, &destlen, wrkmem);
	pfree(wrkmem);

	if (r != LZO_E_OK)
		return -1;

	return destlen;
}

static int32
lzo_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	int r;
	lzo_uint destlen;

	r = lzo1x_decompress((lzo_bytep)source, (lzo_uint)slen,
						 (lzo_bytep)dest, &destlen, NULL);

	if (r != LZO_E_OK)
		return -1;

	return destlen;
}
#endif

#if HAVE_LIBSNAPPY
static int32
snappy_compress_pg(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy)
{
	size_t dlen;

	if (snappy_compress(source, slen, dest, &dlen) != 0)
		return -1;

	/* XXX size_t is >= int32 */
	return dlen;
}

static int32
snappy_decompress_pg(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	size_t destlen;

	if (snappy_uncompress(source, slen, dest, &destlen) != 0)
		return -1;

	return destlen;
}
#endif
