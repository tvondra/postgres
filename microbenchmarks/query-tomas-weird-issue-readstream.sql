-- Tomas' read stream issue
-- Taken from https://postgr.es/m/8f5d66cf-44e9-40e0-8349-d5590ba8efb4@vondra.me
create extension if not exists pg_prewarm;
create extension if not exists pg_buffercache;
set enable_bitmapscan=off;
set enable_seqscan=off;
set max_parallel_workers_per_gather=0;

\echo '#### set enable_indexscan_prefetch = on ####'
select pg_buffercache_evict_relation('t_readstream');
select pg_prewarm('idx_readstream');
\! sudo clear_cache.sh
set enable_indexscan_prefetch = on;
explain (analyze, timing off)
select *
from t_readstream
where
  a between 16150 and 4540437
order by
  a asc;

\echo '#### set enable_indexscan_prefetch = off ####'
select pg_buffercache_evict_relation('t_readstream');
select pg_prewarm('idx_readstream');
\! sudo clear_cache.sh
set enable_indexscan_prefetch = off;
explain (analyze, timing off)
select *
from t_readstream
where
  a between 16150 and 4540437
order by
  a asc;

