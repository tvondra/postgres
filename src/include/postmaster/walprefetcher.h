/*-------------------------------------------------------------------------
 *
 * walprefetcher.h
 *	  Exports from postmaster/walprefetcher.c.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walprefetcher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALPREFETCHER_H
#define _WALPREFETCHER_H

/* GUC options */
extern int			WalPrefetchMinLead;
extern int			WalPrefetchMaxLead;
extern int			WalPrefetchPollInterval;
extern bool 		WalPrefetchEnabled;

extern void WalPrefetcherMain(void) pg_attribute_noreturn();
extern void WalPrefetch(XLogRecPtr lsn);

#endif							/* _WALPREFETCHER_H */
