-- Tomas' read stream issue
-- Taken from https://postgr.es/m/8f5d66cf-44e9-40e0-8349-d5590ba8efb4@vondra.me
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
vacuum freeze ;
analyze t_readstream;
explain (analyze, timing off)
select *
from t_readstream
where
  a between 16150 and 4540437
order by
  a asc;
