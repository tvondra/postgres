/*
 * Copy entire files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_combinebackup/copy_file.h
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/logging.h"
#include "copy_file.h"

static void pg_copyfile(const char *src, const char *dest, const char *addon_errmsg,
						pg_checksum_context *ctx);

static void pg_copyfile_offload(const char *src, const char *dest,
								const char *addon_errmsg, CopyFileMethod flags);

#ifdef WIN32
static void copy_file_copyfile(const char *src, const char *dst);
#endif

/*
 * Copy a regular file, optionally computing a checksum, and emitting
 * appropriate debug messages. But if we're in dry-run mode, then just emit
 * the messages and don't copy anything.
 */
void
copy_file(const char *src, const char *dst,
		  pg_checksum_context *checksum_ctx, bool dry_run,
		  CopyFileMethod copy_strategy)
{
	/*
	 * In dry-run mode, we don't actually copy anything, nor do we read any
	 * data from the source file, but we do verify that we can open it.
	 */
	if (dry_run)
	{
		int			fd;

		if ((fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
			pg_fatal("could not open \"%s\": %m", src);
		if (close(fd) < 0)
			pg_fatal("could not close \"%s\": %m", src);

		return;
	}

	/*
	 * If we don't need to compute a checksum, then we can use any special
	 * operating system primitives that we know about to copy the file; this
	 * may be quicker than a naive block copy.
	 */
	if (checksum_ctx->type == CHECKSUM_TYPE_NONE && copy_strategy != 0)
		pg_copyfile_offload(src, dst, NULL, copy_strategy);
	else
		pg_copyfile(src, dst, NULL, checksum_ctx);
}

/* Helper function to optionally prepend error string */
static inline char *
opt_errinfo(const char *addon_errmsg)
{
	char		buf[128];

	if (addon_errmsg == NULL)
		return "";

	strcpy(buf, " ");

	/* XXX isn't this broken? this returns pointer to local variable */
	return strncat(buf, addon_errmsg, sizeof(buf) - 2);
}

/*
 * Copies a relation file from src to dest. addon_errmsg is an optional
 * addon error message (can be NULL or include schema/relName)
 */
static void
pg_copyfile(const char *src, const char *dest, const char *addon_errmsg,
			pg_checksum_context *ctx)
{
#ifndef WIN32
	int			src_fd;
	int			dest_fd;
	uint8	   *buffer;

	/* XXX where does the 50 blocks come from? larger/smaller? */
	/* copy in fairly large chunks for best efficiency */
	const int	buffer_size = 50 * BLCKSZ;

	if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_fatal("error while copying%s: could not open file \"%s\": %s",
				 opt_errinfo(addon_errmsg), src, strerror(errno));

	if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
						pg_file_create_mode)) < 0)
		pg_fatal("error while copying%s: could not create file \"%s\": %s",
				 opt_errinfo(addon_errmsg), dest, strerror(errno));

	buffer = pg_malloc(buffer_size);

	/* perform data copying i.e read src source, write to destination */
	while (true)
	{
		ssize_t		nbytes = read(src_fd, buffer, buffer_size);

		if (nbytes < 0)
			pg_fatal("error while copying%s: could not read file "
					 "\"%s\": %s",
					 opt_errinfo(addon_errmsg), src, strerror(errno));

		if (nbytes == 0)
			break;

		errno = 0;
		if (write(dest_fd, buffer, nbytes) != nbytes)
		{
			/*
			 * if write didn't set errno, assume problem is no disk space
			 */
			if (errno == 0)
				errno = ENOSPC;
			pg_fatal("error while copying%s: could not write file \"%s\": %s",
					 opt_errinfo(addon_errmsg), dest, strerror(errno));
		}

		if (pg_checksum_update(ctx, buffer, nbytes) < 0)
			pg_fatal("could not calculate checksum of file \"%s\"", dest);
	}

	pg_free(buffer);
	close(src_fd);
	close(dest_fd);

#else							/* WIN32 */
	if (CopyFile(src, dest, true) == 0)
	{
		_dosmaperr(GetLastError());
		pg_fatal("error while copying%s (\"%s\" to \"%s\"): %s", addon_errmsg,
				 opt_errinfo(addon_errmsg), src, dest, strerror(errno));
	}
#endif							/* WIN32 */
}

/*
 * pg_copyfile_offload()
 *
 * Clones/reflinks a relation file from src to dest using variety of methods
 *
 * addon_errmsg can be used to pass additional information in case of errors.
 * flags, see PG_COPYFILE_* enum in file_utils.h
 */
static void
pg_copyfile_offload(const char *src, const char *dest,
					const char *addon_errmsg, CopyFileMethod flags)
{

#ifdef WIN32
	/* on WIN32 we ignore flags, we have no other choice */
	if (CopyFile(src, dest, true) == 0)
	{
		_dosmaperr(GetLastError());
		pg_fatal("error while copying%s (\"%s\" to \"%s\"): %s", addon_errmsg,
				 opt_errinfo(addon_errmsg), src, dest, strerror(errno));
	}
#elif defined(HAVE_COPYFILE) && defined(COPYFILE_CLONE_FORCE)
	/* on MacOS we ignore flags, we have no other choice */
	if (copyfile(src, dest, NULL, COPYFILE_CLONE_FORCE) < 0)
		pg_fatal("error while cloning%s: (\"%s\" to \"%s\"): %s",
				 opt_errinfo(addon_errmsg), src, dest, strerror(errno));

#elif defined(HAVE_COPY_FILE_RANGE) || defined(FICLONE)
	int			src_fd;
	int			dest_fd;
	ssize_t		nbytes;

	if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
		pg_fatal("error while copying%s: could not open file \"%s\": %s",
				 opt_errinfo(addon_errmsg), src, strerror(errno));

	if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
						pg_file_create_mode)) < 0)
		pg_fatal("error while copying%s: could not create file \"%s\": %s",
				 opt_errinfo(addon_errmsg), dest, strerror(errno));

	if (flags & PG_COPYFILE_COPY_FILE_RANGE)
	{
#ifdef HAVE_COPY_FILE_RANGE
		do
		{
			nbytes = copy_file_range(src_fd, NULL, dest_fd, NULL, SSIZE_MAX, 0);
			if (nbytes < 0 && errno != EINTR)
				pg_fatal("error while copying%s: could not copy_file_range()"
						 "from \"%s\" to \"%s\": %s",
						 opt_errinfo(addon_errmsg), src, dest, strerror(errno));
		} while (nbytes > 0);
#else
		pg_fatal("copy file accelaration via copy_file_range() is not supported on "
				 "this platform");
#endif
	}
	else if (flags & PG_COPYFILE_IOCTL_FICLONE)
	{
#if defined(__linux__) && defined(FICLONE)
		if (ioctl(dest_fd, FICLONE, src_fd) < 0)
		{
			int			save_errno = errno;

			unlink(dest);

			pg_fatal("error while cloning%s: (\"%s\" to \"%s\"): %s",
					 opt_errinfo(addon_errmsg), src, dest, strerror(save_errno));
		}
#else
		pg_fatal("clone file accelaration via ioctl(FICLONE) is not supported on "
				 "this platform");
#endif
	}

	close(src_fd);
	close(dest_fd);

#else
	if (flags & PG_COPYFILE_FALLBACK)
		pg_copyfile(src, dest, addon_errmsg);
	else
		pg_fatal("none of the copy file acceleration methods are supported on this "
				 "platform");
#endif
}

#ifdef WIN32
static void
copy_file_copyfile(const char *src, const char *dst)
{
	if (CopyFile(src, dst, true) == 0)
	{
		_dosmaperr(GetLastError());
		pg_fatal("could not copy \"%s\" to \"%s\": %m", src, dst);
	}
}
#endif							/* WIN32 */
