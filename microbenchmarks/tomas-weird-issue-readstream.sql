-- Tomas' read stream issue
-- Taken from https://postgr.es/m/8f5d66cf-44e9-40e0-8349-d5590ba8efb4@vondra.me
drop table if exists t_readstream;
drop table if exists t_tupdistance_new_regress;
create unlogged table t_readstream (a bigint, b text) with (fillfactor = 20);
insert into t_readstream
select
  1 * a,
  b
from (
  select r, a, b, generate_series(0, 2 - 1) as p
  from (
    select
      row_number() over () as r,
      a,
      b
    from (
      select
        i as a,
        md5(i::text) as b
      from
        generate_series(1, 5000000) s(i)
      order by
        (i + 16 *(random() - 0.5))) foo) bar) baz
order by
  ((r * 2 + p) + 8 *(random() - 0.5));
create index idx_readstream on t_readstream(a ASC) with (deduplicate_items=false);
vacuum (analyze, freeze) t_readstream;

create unlogged table t_tupdistance_new_regress(
  a bigint,
  b text
) with ( fillfactor = 20);

insert into t_tupdistance_new_regress
select 1 * a, b
from (
  select
    r,
    a,
    b,
    generate_series(0, 4 - 1) as p
  from (
    select
      row_number() over () as r,
      a,
      b
    from (
      select
        i as a,
        md5(i::text) as b
      from
        generate_series(1, 2500000) s(i)
      order by
        (i + 0 *(random() - 0.5))) foo) bar) baz
order by
  ((r * 4 + p) + 8 *(random() - 0.5));

create index t_tupdistance_new_regress_idx on t_tupdistance_new_regress(a desc) with (deduplicate_items = false);

\echo '#### Now run query-tomas-weird-issue-readstream.sql for query ####'
