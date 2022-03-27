#include "postgres_fe.h"

#include "pg_backup_utils.h"
#include "compress_zstd.h"

#ifndef USE_ZSTD

void
InitCompressorZstd(CompressorState *cs, const pg_compress_specification compression_spec)
{
	pg_fatal("this build does not support compression with %s", "ZSTD");
}

void
InitCompressFileHandleZstd(CompressFileHandle *CFH, const pg_compress_specification compression_spec)
{
	pg_fatal("this build does not support compression with %s", "ZSTD");
}

#else

#include <zstd.h>

typedef struct ZstdCompressorState
{
	/* This is a normal file to which we read/write compressed data */
	FILE	   *fp;
	/* XXX: use one separate ZSTD_CStream per thread: disable on windows ? */
	ZSTD_CStream *cstream;
	ZSTD_DStream *dstream;
	ZSTD_outBuffer output;
	ZSTD_inBuffer input;
}			ZstdCompressorState;

static ZSTD_CStream *ZstdCStreamParams(pg_compress_specification compress);
static void EndCompressorZstd(ArchiveHandle *AH, CompressorState *cs);
static void WriteDataToArchiveZstd(ArchiveHandle *AH, CompressorState *cs,
								   const void *data, size_t dLen);
static void ReadDataFromArchiveZstd(ArchiveHandle *AH, CompressorState *cs);

static void
ZSTD_CCtx_setParam_or_die(ZSTD_CStream * cstream,
						  ZSTD_cParameter param, int value)
{
	size_t		res;

	res = ZSTD_CCtx_setParameter(cstream, param, value);
	if (ZSTD_isError(res))
		pg_fatal("could not set compression parameter: %s",
				 ZSTD_getErrorName(res));
}

/* Return a compression stream with parameters set per argument */
static ZSTD_CStream *
ZstdCStreamParams(pg_compress_specification compress)
{
	ZSTD_CStream *cstream;

	cstream = ZSTD_createCStream();
	if (cstream == NULL)
		pg_fatal("could not initialize compression library");

	ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_compressionLevel,
							  compress.level);

	if (compress.options & PG_COMPRESSION_OPTION_WORKERS)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_nbWorkers,
								  compress.workers);

	if (compress.options & PG_COMPRESSION_OPTION_LONG_DISTANCE)
		ZSTD_CCtx_setParam_or_die(cstream,
				ZSTD_c_enableLongDistanceMatching,
				compress.long_distance);

#if 0
	if (compress.options & PG_COMPRESSION_OPTION_CHECKSUM)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_checksumFlag,
								  compress.checksum);

	/* Still marked as experimental */
	if (compress.options & PG_COMPRESSION_OPTION_RSYNCABLE)
		ZSTD_CCtx_setParam_or_die(cstream, ZSTD_c_rsyncable, 1);
#endif

	return cstream;
}

void
EndCompressorZstd(ArchiveHandle *AH, CompressorState *cs)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) cs->private_data;
	ZSTD_outBuffer *output = &zstdcs->output;

	if (cs->writeF == NULL)
		return;

	for (;;)
	{
		size_t		res;

		output->pos = 0;
		res = ZSTD_compressStream2(zstdcs->cstream, output,
								   &zstdcs->input, ZSTD_e_end);

		if (output->pos > 0)
			cs->writeF(AH, output->dst, output->pos);
		/* TODO check that we wrote "pos" bytes */

		if (ZSTD_isError(res))
			pg_fatal("could not close compression stream: %s",
					 ZSTD_getErrorName(res));

		if (res == 0)
			break;
	}

	/* XXX: retval */
	ZSTD_freeCStream(zstdcs->cstream);
	pg_free(zstdcs->output.dst);
	pg_free(zstdcs);
}

static void
WriteDataToArchiveZstd(ArchiveHandle *AH, CompressorState *cs,
					   const void *data, size_t dLen)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) cs->private_data;
	ZSTD_inBuffer *input = &zstdcs->input;
	ZSTD_outBuffer *output = &zstdcs->output;

	input->src = data;
	input->size = dLen;
	input->pos = 0;

#if 0
	ZSTD_CCtx_reset(zstdcs->cstream, ZSTD_reset_session_only); // XXX */
	res = ZSTD_CCtx_setPledgedSrcSize(cs->zstd.cstream, dLen);
	if (ZSTD_isError(res))
	pg_fatal("could not compress data: %s", ZSTD_getErrorName(res));
#endif

	while (input->pos != input->size)
	{
		size_t		res;

		res = ZSTD_compressStream2(zstdcs->cstream, output,
								   input, ZSTD_e_continue);

		if (output->pos == output->size ||
			input->pos != input->size)
		{
			/*
			 * Extra paranoia: avoid zero-length chunks, since a zero length
			 * chunk is the EOF marker in the custom format. This should never
			 * happen but...
			 */
			if (output->pos > 0)
				cs->writeF(AH, output->dst, output->pos);

			output->pos = 0;
		}

		if (ZSTD_isError(res))
			pg_fatal("could not compress data: %s", ZSTD_getErrorName(res));
	}
}

/* Read data from a compressed zstd archive */
static void
ReadDataFromArchiveZstd(ArchiveHandle *AH, CompressorState *cs)
{
	ZSTD_DStream *dstream;
	ZSTD_outBuffer output;
	ZSTD_inBuffer input;
	size_t		res;
	size_t		input_size;

	dstream = ZSTD_createDStream();
	if (dstream == NULL)
		pg_fatal("could not initialize compression library");

	input_size = ZSTD_DStreamInSize();
	input.src = pg_malloc(input_size);

	output.size = ZSTD_DStreamOutSize();
	output.dst = pg_malloc(output.size);

	/* read compressed data */
	for (;;)
	{
		size_t		cnt;

		/*
		 * XXX: the buffer can grow, we shouldn't keep resetting it to the
		 * original value..
		 */
		input.size = input_size;

		cnt = cs->readF(AH, (char **) unconstify(void **, &input.src), &input.size);
		input.pos = 0;
		input.size = cnt;

		if (cnt == 0)
			break;

		while (input.pos < input.size)
		{
			/* decompress */
			output.pos = 0;
			res = ZSTD_decompressStream(dstream, &output, &input);

			if (ZSTD_isError(res))
				pg_fatal("could not decompress data: %s", ZSTD_getErrorName(res));

			/* write to output handle */
			((char *) output.dst)[output.pos] = '\0';
			ahwrite(output.dst, 1, output.pos, AH);
			/* if (res == 0) break; */
		}
	}

	pg_free(unconstify(void *, input.src));
	pg_free(output.dst);
}

/* Public routines that support Zstd compressed data I/O */
void
InitCompressorZstd(CompressorState *cs, const pg_compress_specification compression_spec)
{
	ZstdCompressorState *zstdcs;

	cs->readData = ReadDataFromArchiveZstd;
	cs->writeData = WriteDataToArchiveZstd;
	cs->end = EndCompressorZstd;

	cs->compression_spec = compression_spec;

	cs->private_data = pg_malloc0(sizeof(ZstdCompressorState));
	zstdcs = cs->private_data;
	/* XXX: initialize safely like the corresponding zlib "paranoia" */
	zstdcs->output.size = ZSTD_CStreamOutSize();
	zstdcs->output.dst = pg_malloc(zstdcs->output.size);
	zstdcs->output.pos = 0;
	zstdcs->cstream = ZstdCStreamParams(cs->compression_spec);
}

/*----------------------
 * Compress File API
 *----------------------
 */

static size_t
Zstd_read(void *ptr, size_t size, CompressFileHandle *CFH)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;
	ZSTD_inBuffer *input = &zstdcs->input;
	ZSTD_outBuffer *output = &zstdcs->output;
	size_t		input_size = ZSTD_DStreamInSize();

	/* input_size is the allocated size */
	size_t		res,
				cnt;

	output->size = size;
	output->dst = ptr;
	output->pos = 0;

	for (;;)
	{
		Assert(input->pos <= input->size);
		Assert(input->size <= input_size);

		/* If the input is completely consumed, start back at the beginning */
		if (input->pos == input->size)
		{
			/* input->size is size produced by "fread" */
			input->size = 0;
			/* input->pos is position consumed by decompress */
			input->pos = 0;
		}

		/* read compressed data if we must produce more input */
		if (input->pos == input->size)
		{
			cnt = fread(unconstify(void *, input->src), 1, input_size, zstdcs->fp);
			input->size = cnt;

			/* If we have no input to consume, we're done */
			if (cnt == 0)
				break;
		}

		Assert(cnt >= 0);
		Assert(input->size <= input_size);

		/* Now consume as much as possible */
		for (; input->pos < input->size;)
		{
			/* decompress */
			res = ZSTD_decompressStream(zstdcs->dstream, output, input);
			if (ZSTD_isError(res))
				pg_fatal("could not decompress data: %s", ZSTD_getErrorName(res));
			if (output->pos == output->size)
				break;			/* No more room for output */
			if (res == 0)
				break;			/* End of frame */
		}

		if (output->pos == output->size)
			break;				/* We read all the data that fits */
	}

	return output->pos;
}

static size_t
Zstd_write(const void *ptr, size_t size, CompressFileHandle *CFH)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;
	ZSTD_inBuffer *input = &zstdcs->input;
	ZSTD_outBuffer *output = &zstdcs->output;
	size_t		res,
				cnt;

	input->src = ptr;
	input->size = size;
	input->pos = 0;

#if 0
	ZSTD_CCtx_reset(fp->zstd.cstream, ZSTD_reset_session_only);
	res = ZSTD_CCtx_setPledgedSrcSize(fp->zstd.cstream, size);
	if (ZSTD_isError(res))
	pg_fatal("could not compress data: %s", ZSTD_getErrorName(res));
#endif

	/* Consume all input, and flush later */
	while (input->pos != input->size)
	{
		output->pos = 0;
		res = ZSTD_compressStream2(zstdcs->cstream, output, input, ZSTD_e_continue);
		if (ZSTD_isError(res))
			pg_fatal("could not compress data: %s", ZSTD_getErrorName(res));

		cnt = fwrite(output->dst, 1, output->pos, zstdcs->fp);
		if (cnt != output->pos)
			pg_fatal("could not write data: %m");
	}

	return size;
}

static int
Zstd_getc(CompressFileHandle *CFH)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;
	int			ret;

	if (CFH->read_func(&ret, 1, CFH) != 1)
	{
		if (feof(zstdcs->fp))
			pg_fatal("could not read from input file: end of file");
		else
			pg_fatal("could not read from input file: %m");
	}
	return ret;
}

static char *
Zstd_gets(char *buf, int len, CompressFileHandle *CFH)
{
	/*
	 * Read one byte at a time until newline or EOF. This is only used to read
	 * the list of blobs, and the I/O is buffered anyway.
	 */
	int			i,
				res;

	for (i = 0; i < len - 1; ++i)
	{
		res = CFH->read_func(&buf[i], 1, CFH);
		if (res != 1)
			break;
		if (buf[i] == '\n')
		{
			++i;
			break;
		}
	}
	buf[i] = '\0';
	return i > 0 ? buf : 0;
}

static int
Zstd_close(CompressFileHandle *CFH)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;
	int			result;

	if (zstdcs->cstream)
	{
		size_t		res,
					cnt;
		ZSTD_inBuffer *input = &zstdcs->input;
		ZSTD_outBuffer *output = &zstdcs->output;

		for (;;)
		{
			output->pos = 0;
			res = ZSTD_compressStream2(zstdcs->cstream, output, input, ZSTD_e_end);
			if (ZSTD_isError(res))
				pg_fatal("could not compress data: %s", ZSTD_getErrorName(res));

			cnt = fwrite(output->dst, 1, output->pos, zstdcs->fp);
			if (cnt != output->pos)
				pg_fatal("could not write data: %m");

			if (res == 0)
				break;
		}

		ZSTD_freeCStream(zstdcs->cstream);
		pg_free(zstdcs->output.dst);
	}

	if (zstdcs->dstream)
	{
		ZSTD_freeDStream(zstdcs->dstream);
		pg_free(unconstify(void *, zstdcs->input.src));
	}

	result = fclose(zstdcs->fp);
	pg_free(zstdcs);
	return result;
}

static int
Zstd_eof(CompressFileHandle *CFH)
{
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;

	return feof(zstdcs->fp);
}

static int
Zstd_open(const char *path, int fd, const char *mode,
		  CompressFileHandle *CFH)
{
	FILE	   *fp;
	ZstdCompressorState *zstdcs;

	if (fd >= 0)
		fp = fdopen(fd, mode);
	else
		fp = fopen(path, mode);

	if (fp == NULL)
	{
		/* XXX zstdcs->errcode = errno; */
		return 1;
	}

	CFH->private_data = pg_malloc0(sizeof(ZstdCompressorState));
	zstdcs = (ZstdCompressorState *) CFH->private_data;
	zstdcs->fp = fp;

	if (mode[0] == 'w' || mode[0] == 'a')
	{
		zstdcs->output.size = ZSTD_CStreamOutSize();
		zstdcs->output.dst = pg_malloc0(zstdcs->output.size);
		zstdcs->cstream = ZstdCStreamParams(CFH->compression_spec);
	}
	else if (strchr(mode, 'r'))
	{
		zstdcs->input.src = pg_malloc0(ZSTD_DStreamInSize());
		zstdcs->dstream = ZSTD_createDStream();
		if (zstdcs->dstream == NULL)
			pg_fatal("could not initialize compression library");
	}
	/* XXX else: bad mode */

	return 0;
}

static int
Zstd_open_write(const char *path, const char *mode, CompressFileHandle *CFH)
{
	char		fname[MAXPGPATH];

	sprintf(fname, "%s.zst", path);
	return CFH->open_func(fname, -1, mode, CFH);
}

static const char *
Zstd_get_error(CompressFileHandle *CFH)
{
#if 0
	ZstdCompressorState *zstdcs = (ZstdCompressorState *) CFH->private_data;

	if (ZSTD_isError(res))
		return ZSTD_getErrorName(res)
	else
#endif

	return strerror(errno);
}

void
InitCompressFileHandleZstd(CompressFileHandle *CFH, const pg_compress_specification compression_spec)
{
	CFH->open_func = Zstd_open;
	CFH->open_write_func = Zstd_open_write;
	CFH->read_func = Zstd_read;
	CFH->write_func = Zstd_write;
	CFH->gets_func = Zstd_gets;
	CFH->getc_func = Zstd_getc;
	CFH->close_func = Zstd_close;
	CFH->eof_func = Zstd_eof;
	CFH->get_error_func = Zstd_get_error;

	CFH->compression_spec = compression_spec;

	CFH->private_data = NULL;
}

#endif							/* USE_ZSTD */
