--
-- Benchmark to showcase index IO prefetching benefits
--
-- Idempotent: skips data loading if tables exist and have data.
-- Will reload if tables are empty (e.g., after crash with unlogged tables).
--

create extension if not exists pg_prewarm;
create extension if not exists pg_buffercache;

set enable_bitmapscan = off;
set enable_seqscan = off;
set max_parallel_workers_per_gather = 0;

--
-- Check if we need to load data
-- Tables must exist AND have data (unlogged tables are empty after crash)
--
select not exists (
  select 1 from pg_class where relname = 'prefetch_orders' and relkind = 'r'
) as table_missing \gset

\if :table_missing
select true as needs_load \gset
\else
select (select count(*) = 0 from prefetch_orders) as needs_load \gset
\endif

\if :needs_load

\echo ''
\echo '=========================================='
\echo 'Loading data (tables missing or empty)...'
\echo '=========================================='
\echo ''

--
-- Table setup
--

drop table if exists prefetch_orders cascade;
drop table if exists prefetch_customers cascade;
drop table if exists prefetch_products cascade;

-- Main fact table: ~50M rows with low fillfactor for more rows per block
create unlogged table prefetch_orders (
  order_id bigint,
  customer_id int,
  product_id int,
  order_date date,
  region_id int,
  amount numeric(10,2)
) with (fillfactor = 40);

-- Dimension table: customers
create unlogged table prefetch_customers (
  customer_id int primary key,
  region_id int,
  customer_name text
);

-- Dimension table: products
create unlogged table prefetch_products (
  product_id int primary key,
  category_id int,
  product_name text
);

--
-- Data loading - deterministic with setseed
--

\echo 'Loading dimension tables...'

-- Load customers (100K)
insert into prefetch_customers (customer_id, region_id, customer_name)
select
  i,
  (i % 20) + 1,
  'Customer_' || i
from generate_series(1, 100000) i;

-- Load products (10K)
insert into prefetch_products (product_id, category_id, product_name)
select
  i,
  (i % 50) + 1,
  'Product_' || i
from generate_series(1, 10000) i;

\echo 'Loading orders table (~50M rows)...'
\echo 'This will take several minutes...'

-- Set deterministic seed
select setseed(0.5);

-- Load orders with controlled scatter pattern
-- Groups of ~32 rows with similar customer_id land on same heap pages,
-- but pages are scattered across the heap. This creates ideal prefetch
-- conditions: index scan produces TIDs where consecutive entries point
-- to the same block, but blocks are accessed "randomly".
insert into prefetch_orders (order_id, customer_id, product_id, order_date, region_id, amount)
select
  row_number() over () as order_id,
  customer_id,
  product_id,
  order_date,
  (customer_id % 20) + 1 as region_id,
  (random() * 1000)::numeric(10,2) as amount
from (
  select
    ((g.i - 1) % 100000) + 1 as customer_id,
    ((g.i - 1) % 10000) + 1 as product_id,
    '2023-01-01'::date + ((g.i - 1) % 730) as order_date
  from generate_series(1, 50000000) g(i)
  order by (g.i / 32) + (random() * 4 - 2)::int
) sub;

\echo 'Creating indexes...'

-- Index for customer + date range queries (primary for join demos)
create index prefetch_orders_cust_date_idx
  on prefetch_orders(customer_id, order_date)
  with (deduplicate_items=off);

-- Index for date-only queries
create index prefetch_orders_date_idx
  on prefetch_orders(order_date)
  with (deduplicate_items=off);

-- Index for product joins
create index prefetch_orders_prod_idx
  on prefetch_orders(product_id)
  with (deduplicate_items=off);

-- Index for order_id (sequential access adversarial test)
create index prefetch_orders_id_idx
  on prefetch_orders(order_id)
  with (deduplicate_items=off);

\echo 'Running VACUUM FREEZE ANALYZE...'
vacuum freeze analyze prefetch_orders;
vacuum freeze analyze prefetch_customers;
vacuum freeze analyze prefetch_products;

checkpoint;

\else

\echo ''
\echo '=========================================='
\echo 'Tables exist with data, skipping load.'
\echo '=========================================='

-- Ensure order_id index exists (added later for adversarial test)
create index if not exists prefetch_orders_id_idx
  on prefetch_orders(order_id)
  with (deduplicate_items=off);

\endif

\echo ''
\echo '=========================================='
\echo 'Running benchmark queries.'
\echo '=========================================='
\echo ''

--------------------------------------------------------------------------------
-- Query 1: Simple date range scan
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 1: Simple date range scan            #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_id, customer_id, amount
from prefetch_orders
where order_date between '2023-06-01' and '2023-06-15'
order by order_date
limit 50000;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_id, customer_id, amount
from prefetch_orders
where order_date between '2023-06-01' and '2023-06-15'
order by order_date
limit 50000;

--------------------------------------------------------------------------------
-- Query 2: JOIN - Orders with Customers (SHOWCASE JOIN QUERY)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 2: JOIN Orders + Customers           #'
\echo '# (Showcase join query with prefetch)        #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, o.amount, o.order_date
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
where o.customer_id between 5000 and 5500
  and o.order_date between '2023-06-01' and '2023-06-30'
order by o.order_date;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, o.amount, o.order_date
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
where o.customer_id between 5000 and 5500
  and o.order_date between '2023-06-01' and '2023-06-30'
order by o.order_date;

--------------------------------------------------------------------------------
-- Query 3: Multi-table JOIN - Orders + Customers + Products (SHOWCASE)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 3: Multi-JOIN Orders+Customers+Prods #'
\echo '# (Showcase multi-table join with prefetch)  #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_buffercache_evict_relation('prefetch_products');
select pg_prewarm('prefetch_orders_date_idx');
select pg_prewarm('prefetch_customers_pkey');
select pg_prewarm('prefetch_products_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, p.product_name, o.amount
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
join prefetch_products p on p.product_id = o.product_id
where o.order_date between '2023-03-01' and '2023-03-15'
order by o.order_date, o.order_id
limit 100000;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_buffercache_evict_relation('prefetch_products');
select pg_prewarm('prefetch_orders_date_idx');
select pg_prewarm('prefetch_customers_pkey');
select pg_prewarm('prefetch_products_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, p.product_name, o.amount
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
join prefetch_products p on p.product_id = o.product_id
where o.order_date between '2023-03-01' and '2023-03-15'
order by o.order_date, o.order_id
limit 100000;

--------------------------------------------------------------------------------
-- Query 4: Aggregation with IN list on customer_id
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 4: Aggregation with IN list          #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_date, sum(amount) as total, count(*) as orders
from prefetch_orders
where customer_id in (1000, 2000, 3000, 4000, 5000,
                      6000, 7000, 8000, 9000, 10000)
  and order_date between '2023-01-01' and '2023-12-31'
group by order_date
order by order_date;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_date, sum(amount) as total, count(*) as orders
from prefetch_orders
where customer_id in (1000, 2000, 3000, 4000, 5000,
                      6000, 7000, 8000, 9000, 10000)
  and order_date between '2023-01-01' and '2023-12-31'
group by order_date
order by order_date;

--------------------------------------------------------------------------------
-- Query 5: Backwards scan with join (disable sort to force backwards index scan)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 5: Backwards scan with JOIN          #'
\echo '# (enable_sort=off forces backwards scan)    #'
\echo '###############################################'
\echo ''

set enable_sort = off;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, o.amount, o.order_date
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
where o.customer_id between 10000 and 10500
  and o.order_date between '2023-09-01' and '2023-09-30'
order by o.customer_id desc, o.order_date desc;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select o.order_id, c.customer_name, o.amount, o.order_date
from prefetch_orders o
join prefetch_customers c on c.customer_id = o.customer_id
where o.customer_id between 10000 and 10500
  and o.order_date between '2023-09-01' and '2023-09-30'
order by o.customer_id desc, o.order_date desc;

set enable_sort = on;

--------------------------------------------------------------------------------
-- Query 6: Nested Loop Join with correlated subquery
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 6: Nested Loop with correlated subq  #'
\echo '# (Small outer drives repeated index scans)  #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, (
  select sum(amount) from prefetch_orders o
  where o.customer_id = c.customer_id
)
from prefetch_customers c
where c.customer_id between 1000 and 1100;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, (
  select sum(amount) from prefetch_orders o
  where o.customer_id = c.customer_id
)
from prefetch_customers c
where c.customer_id between 1000 and 1100;

--------------------------------------------------------------------------------
-- Query 7: LIMIT with large OFFSET (skip scan pattern)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 7: LIMIT with large OFFSET           #'
\echo '# (Must scan many TIDs before returning)     #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 5000
order by order_date
offset 400 limit 100;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 5000
order by order_date
offset 400 limit 100;

--------------------------------------------------------------------------------
-- Query 8: Semi-join with EXISTS
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 8: Semi-join with EXISTS             #'
\echo '# (Index scan driven by outer table)         #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_id, c.customer_name
from prefetch_customers c
where exists (
  select 1 from prefetch_orders o
  where o.customer_id = c.customer_id
    and o.order_date between '2023-07-01' and '2023-07-31'
    and o.amount > 900
)
and c.region_id = 5;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_id, c.customer_name
from prefetch_customers c
where exists (
  select 1 from prefetch_orders o
  where o.customer_id = c.customer_id
    and o.order_date between '2023-07-01' and '2023-07-31'
    and o.amount > 900
)
and c.region_id = 5;

--------------------------------------------------------------------------------
-- Query 9: Lateral join (Top N per group)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 9: Lateral join (Top N per group)    #'
\echo '# (Correlated index scans per customer)      #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, recent.*
from prefetch_customers c,
lateral (
  select order_id, order_date, amount
  from prefetch_orders o
  where o.customer_id = c.customer_id
  order by o.order_date desc
  limit 5
) recent
where c.customer_id between 2000 and 2100;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, recent.*
from prefetch_customers c,
lateral (
  select order_id, order_date, amount
  from prefetch_orders o
  where o.customer_id = c.customer_id
  order by o.order_date desc
  limit 5
) recent
where c.customer_id between 2000 and 2100;

--------------------------------------------------------------------------------
-- Query 10: Range scan with selective filter (late materialization)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Query 10: Range scan + selective filter    #'
\echo '# (Wide range, selective non-indexed filter) #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_id, customer_id, amount
from prefetch_orders
where order_date between '2023-01-01' and '2023-12-31'
  and amount between 999.00 and 999.99;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select order_id, customer_id, amount
from prefetch_orders
where order_date between '2023-01-01' and '2023-12-31'
  and amount between 999.00 and 999.99;

\echo ''
\echo '=========================================='
\echo 'ADVERSARIAL QUERIES (prefetch may hurt)  '
\echo '=========================================='
\echo ''

--------------------------------------------------------------------------------
-- Adversarial 1: Sequential heap access (table clustered on index)
-- Create a small table where heap is physically sequential with the index order.
-- Prefetching adds overhead but OS readahead already handles sequential IO well.
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 1: Sequential heap access      #'
\echo '# (heap physically ordered by index key)     #'
\echo '###############################################'
\echo ''

-- Create a clustered table for this test (small, to avoid long setup)
drop table if exists prefetch_sequential;
create unlogged table prefetch_sequential (
  id bigint,
  val1 int,
  val2 text
);
-- Insert in order so heap is sequential
insert into prefetch_sequential
select i, i % 1000, 'value_' || i
from generate_series(1, 500000) i;
-- Index on id - heap is already in id order
create index prefetch_sequential_idx on prefetch_sequential(id);
vacuum analyze prefetch_sequential;

select pg_buffercache_evict_relation('prefetch_sequential');
select pg_prewarm('prefetch_sequential_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_sequential
where id between 1 and 200000
order by id;

select pg_buffercache_evict_relation('prefetch_sequential');
select pg_prewarm('prefetch_sequential_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_sequential
where id between 1 and 200000
order by id;

--------------------------------------------------------------------------------
-- Adversarial 2: Very small result set (prefetch window never fills)
-- Use a customer with very few orders and a narrow date range.
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 2: Tiny result set             #'
\echo '# (Only a few rows, prefetch is wasted work) #'
\echo '###############################################'
\echo ''

-- Each customer has ~500 orders (50M / 100K customers), spread across 730 days.
-- A 1-day window gives ~0.7 rows on average. Use 3-day window for small but non-empty result.
select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 50000
  and order_date between '2023-06-15' and '2023-06-17';

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_cust_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 50000
  and order_date between '2023-06-15' and '2023-06-17';

--------------------------------------------------------------------------------
-- Adversarial 3: High cache hit ratio (both runs use cached data)
-- Prefetching overhead without IO benefit
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 3: High cache hit ratio        #'
\echo '# (Data already in buffer cache)             #'
\echo '###############################################'
\echo ''

-- Warm up by running the exact query twice (loads into shared_buffers)
select * from prefetch_orders
where customer_id = 1
  and order_date between '2023-01-01' and '2023-12-31';
select * from prefetch_orders
where customer_id = 1
  and order_date between '2023-01-01' and '2023-12-31';

-- NO eviction, NO clear_cache.sh - data stays in buffer cache
\echo '#### enable_indexscan_prefetch = on (cached) ####'
set enable_indexscan_prefetch = on;
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 1
  and order_date between '2023-01-01' and '2023-12-31';

-- Data still cached, no eviction between runs
\echo '#### enable_indexscan_prefetch = off (cached) ####'
set enable_indexscan_prefetch = off;
explain (analyze, costs off, timing off)
select * from prefetch_orders
where customer_id = 1
  and order_date between '2023-01-01' and '2023-12-31';

--------------------------------------------------------------------------------
-- Adversarial 4: Early LIMIT termination (prefetched blocks never used)
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 4: Early LIMIT termination     #'
\echo '# (Prefetches ahead but stops early)         #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where order_date between '2023-01-01' and '2023-12-31'
order by order_date
limit 10;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_prewarm('prefetch_orders_date_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select * from prefetch_orders
where order_date between '2023-01-01' and '2023-12-31'
order by order_date
limit 10;

--------------------------------------------------------------------------------
-- Adversarial 5: Sparse index with 1 TID per heap block
-- Each index entry points to a unique heap block. Prefetching still works
-- but there's no batching benefit (no multiple TIDs per block to amortize).
-- This is the scenario where prefetch overhead might show without the
-- benefit of reduced IO latency from batching.
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 5: Sparse - 1 TID per block    #'
\echo '# (No batching benefit, just prefetch queue) #'
\echo '###############################################'
\echo ''

-- Create a table with one row per 8KB block (very sparse)
-- fillfactor doesn't help here, we just insert one row and move on
drop table if exists prefetch_sparse;
create unlogged table prefetch_sparse (
  id bigint,
  category int,
  padding text  -- make each row ~4KB so only 1-2 fit per block
);
alter table prefetch_sparse alter column padding set storage plain;

insert into prefetch_sparse
select
  i,
  (i % 50) + 1,  -- 50 categories
  repeat('x', 4000)  -- ~4KB padding
from generate_series(1, 50000) i
-- Randomize insertion order so heap blocks are scattered by category
order by random();

create index prefetch_sparse_cat_idx on prefetch_sparse(category);
vacuum analyze prefetch_sparse;

-- Query scans multiple leaf pages, each TID goes to different heap block
select pg_buffercache_evict_relation('prefetch_sparse');
select pg_prewarm('prefetch_sparse_cat_idx');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select id, category from prefetch_sparse
where category between 10 and 20;

select pg_buffercache_evict_relation('prefetch_sparse');
select pg_prewarm('prefetch_sparse_cat_idx');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select id, category from prefetch_sparse
where category between 10 and 20;

--------------------------------------------------------------------------------
-- Adversarial 6: Nested loop inner with LIMIT 1 per iteration
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 6: NL inner with LIMIT 1       #'
\echo '# (Rescan after 1 row, wasted prefetch)      #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, (
  select amount from prefetch_orders o
  where o.customer_id = c.customer_id
  order by o.order_date desc
  limit 1
)
from prefetch_customers c
where c.customer_id between 1000 and 2000;

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_name, (
  select amount from prefetch_orders o
  where o.customer_id = c.customer_id
  order by o.order_date desc
  limit 1
)
from prefetch_customers c
where c.customer_id between 1000 and 2000;

--------------------------------------------------------------------------------
-- Adversarial 7: Index Only Scan with no heap fetches
--------------------------------------------------------------------------------

\echo ''
\echo '###############################################'
\echo '# Adversarial 7: Index Only Scan (no heap)   #'
\echo '###############################################'
\echo ''

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = on ####'
set enable_indexscan_prefetch = on;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_id, c.customer_name
from prefetch_customers c
where not exists (
  select 1 from prefetch_orders o
  where o.customer_id = c.customer_id
    and o.order_date between '2023-12-01' and '2023-12-31'
);

select pg_buffercache_evict_relation('prefetch_orders');
select pg_buffercache_evict_relation('prefetch_customers');
select pg_prewarm('prefetch_orders_cust_date_idx');
select pg_prewarm('prefetch_customers_pkey');
\echo '#### enable_indexscan_prefetch = off ####'
set enable_indexscan_prefetch = off;
\! sudo clear_cache.sh
explain (analyze, costs off, timing off)
select c.customer_id, c.customer_name
from prefetch_customers c
where not exists (
  select 1 from prefetch_orders o
  where o.customer_id = c.customer_id
    and o.order_date between '2023-12-01' and '2023-12-31'
);

\echo ''
\echo '=========================================='
\echo 'Benchmark complete.'
\echo '=========================================='
