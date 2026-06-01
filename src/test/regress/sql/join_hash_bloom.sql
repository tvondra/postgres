CREATE TABLE hash_bloom_fact (id int, did int, padding text);
CREATE TABLE hash_bloom_dimension (id int, r float, padding text);

-- fact is 10x the dimension size
SELECT setseed(0); -- stabilize random() output
INSERT INTO hash_bloom_fact SELECT i, 1 + mod(i, 10000), md5(i::text) FROM generate_series(1,100000) s(i);
INSERT INTO hash_bloom_dimension SELECT i, random(), md5(i::text) FROM generate_series(1,10000) s(i);

VACUUM ANALYZE hash_bloom_fact;
VACUUM ANALYZE hash_bloom_dimension;

-- no parallel queries for now, force hashjoins
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET work_mem = '512kB';

-- non-selective in-memory hash join does not use Bloom filters

SET enable_hashjoin_bloom = off;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id);

SET enable_hashjoin_bloom = on;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id);

-- a selective in-memory join uses a filter (after 1000 lookups)

SET enable_hashjoin_bloom = off;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5;

SET enable_hashjoin_bloom = on;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5;

-- force batching
SET work_mem = '128kB';

-- batched join always creates a Bloom filter, but then disables it if
-- not selective enough

SET enable_hashjoin_bloom = off;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id);

SET enable_hashjoin_bloom = on;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id);

-- batched join always creates a Bloom filter, and keeps using it if
-- selective enough

SET enable_hashjoin_bloom = off;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5;

SET enable_hashjoin_bloom = on;
EXPLAIN (ANALYZE, VERBOSE, TIMING OFF, COSTS OFF, BUFFERS OFF, SUMMARY OFF) SELECT * FROM hash_bloom_fact f JOIN hash_bloom_dimension d ON (f.did = d.id) WHERE d.r < 0.5;
