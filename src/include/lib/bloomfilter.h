/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/lib/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
								  uint64 seed);
extern bloom_filter *bloom_create_custom(int64 total_elems, int bloom_work_mem,
										 uint64 min_bitset_bytes, uint64 seed);
extern size_t bloom_estimate(int64 total_elems, int bloom_work_mem);
extern size_t bloom_estimate_custom(int64 total_elems, int bloom_work_mem,
									Size min_filter_size);
extern bloom_filter *bloom_init(void *ptr, int64 total_elems,
								int bloom_work_mem, uint64 seed);
extern bloom_filter *bloom_init_custom(void *ptr, int64 total_elems,
									   int bloom_work_mem,
									   Size min_filter_size, uint64 seed);
extern void bloom_merge(bloom_filter *dst, const bloom_filter *src);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
							  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
								size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);
extern int bloom_num_hash_funcs(bloom_filter *filter);
extern uint64 bloom_total_bits(bloom_filter *filter);
extern double bloom_false_positive_rate(bloom_filter *filter);

#endif							/* BLOOMFILTER_H */
