/* ----------
 * pg_lzcompress.h -
 *
 *	Definitions for the builtin LZ compressor
 *
 * src/include/common/pg_lzcompress.h
 * ----------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/pg_lzcompress.h"

#ifndef _PG_COMPRESSION_H_
#define _PG_COMPRESSION_H_

#define COMPRESSION_PGLZ	1
#define COMPRESSION_LZ4		2
#define COMPRESSION_LZ4HC	3
#define COMPRESSION_LZO		4
#define COMPRESSION_SNAPPY	5
#define COMPRESSION_NONE	6

extern int compression_algorithm;

extern int32 pg_compress(const char *source, int32 slen, char *dest,
			  const PGLZ_Strategy *strategy);

extern int32 pg_decompress(const char *source, int32 slen, char *dest,
			  int32 rawsize);

/*
 * Get maximum limit for all the (supported) algorithms (need provably
 * static value, because of xloginsert.c).
 */
#define COMPRESSION_MAX_OUTPUT(_dlen) \
	MAX(MAX(PGLZ_MAX_OUTPUT(_dlen), \
	        LZ4_MAX_COMPRESSED_SIZE(_dlen)), \
	MAX(LZO2_MAX_COMPRESSED_SIZE(_dlen), \
	    SNAPPY_MAX_COMPRESSED_SIZE(_dlen)))

#if HAVE_LIBLZ4
#define LZ4_MAX_COMPRESSED_SIZE(_dlen) \
	((unsigned int)(_dlen) > (unsigned int)0x7E000000 ? 0 : (_dlen) + ((_dlen)/255) + 16) /* LZ4_compressBound(_dlen) */
#else
#define LZ4_MAX_COMPRESSED_SIZE(_dlen)		(0)
#endif

#if HAVE_LIBLZO2
#define LZO2_MAX_COMPRESSED_SIZE(_dlen) \
	(_dlen + _dlen / 16 + 64 + 3)
#else
#define LZO2_MAX_COMPRESSED_SIZE(_dlen)		(0)
#endif

#if HAVE_LIBSNAPPY
#define SNAPPY_MAX_COMPRESSED_SIZE(_dlen) \
	(32 + _dlen + _dlen / 6)	/* snappy_max_compressed_length(_dlen) */
#else
#define SNAPPY_MAX_COMPRESSED_SIZE(_dlen)	(0)
#endif

#define MAX(a,b) ((a>b) ? (a) : (b))

#endif   /* _PG_COMPRESSION_H_ */
