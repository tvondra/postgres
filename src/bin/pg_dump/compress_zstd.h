#ifndef COMPRESS_ZSTD_H
#define COMPRESS_ZSTD_H

#include "compress_io.h"

extern void InitCompressorZstd(CompressorState *cs, const pg_compress_specification compression_spec);
extern void InitCompressFileHandleZstd(CompressFileHandle *CFH, const pg_compress_specification compression_spec);

#endif /* COMPRESS_ZSTD_H */
