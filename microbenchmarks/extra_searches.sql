set client_min_messages=error;
drop table if exists redescend_test;
reset client_min_messages;
create unlogged table redescend_test (district int4, warehouse int4, orderid int4, orderline int4);
create index must_not_full_scan on redescend_test (district, warehouse, orderid, orderline) with (fillfactor=30);
insert into redescend_test
select district, warehouse, orderid, orderline
from
  generate_series(1, 3) district,
  generate_series(1, 5) warehouse,
  generate_series(1, 150) orderid,
  generate_series(1, 10) orderline
order by
district, warehouse, orderid, orderline;

vacuum freeze redescend_test;
set enable_indexonlyscan = off;

set enable_indexscan_prefetch to on;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF)
-- declare default_scroll_cursor cursor for
select * from redescend_test
where district in (1, 3) and warehouse in (3, 7)
and orderid >= 42 and orderid <= 44
and orderline = 5
order by district, warehouse, orderid, orderline;

set enable_indexscan_prefetch to off;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF)
-- declare default_scroll_cursor cursor for
select * from redescend_test
where district in (1, 3) and warehouse in (3, 7)
and orderid >= 42 and orderid <= 44
and orderline = 5
order by district, warehouse, orderid, orderline;
