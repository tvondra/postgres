/*-------------------------------------------------------------------------
 *
 * pg_numa.c
 * 		Basic NUMA portability routines
 *
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_numa.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <unistd.h>

#ifdef WIN32
#include <windows.h>
#endif

#include "fmgr.h"
#include "miscadmin.h"
#include "port/pg_numa.h"
#include "storage/pg_shmem.h"

/*
 * At this point we provide support only for Linux thanks to libnuma, but in
 * future support for other platforms e.g. Win32 or FreeBSD might be possible
 * too. For Win32 NUMA APIs see
 * https://learn.microsoft.com/en-us/windows/win32/procthread/numa-support
 */
#ifdef USE_LIBNUMA

#include <numa.h>
#include <numaif.h>

/* libnuma requires initialization as per numa(3) on Linux */
int
pg_numa_init(void)
{
	int			r = numa_available();

	return r;
}

int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	return numa_move_pages(pid, count, pages, NULL, status, 0);
}

int
pg_numa_get_max_node(void)
{
	return numa_max_node();
}

#else

/* Empty wrappers */
int
pg_numa_init(void)
{
	/* We state that NUMA is not available */
	return -1;
}

int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	return 0;
}

int
pg_numa_get_max_node(void)
{
	return 0;
}

#endif

Datum
pg_numa_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(pg_numa_init() != -1);
}

/* This should be used only after the server is started */
Size
pg_numa_get_pagesize(void)
{
	Size		os_page_size;
#ifdef WIN32
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	os_page_size = sysinfo.dwPageSize;
#else
	os_page_size= sysconf(_SC_PAGESIZE);
#endif

	Assert(IsUnderPostmaster);

	if (huge_pages_status == HUGE_PAGES_ON)
		GetHugePageSize(&os_page_size, NULL);

	return os_page_size;
}
