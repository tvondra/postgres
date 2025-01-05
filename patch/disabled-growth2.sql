drop table if exists t;

create table t (a int, b text);

-- make sure the hash has first 3 bits set to 0
insert into t select i a, md5(i::text) from generate_series(1,1000000) s(i) where hashint4(i)::bit(32) & 0xE0000000::bit(32) = 0x00000000::bit(32);

insert into t select * from t;
insert into t select * from t;

alter table t add column r1 double precision, add column r2 double precision, add column r3 double precision, add column r4 double precision;
update t set r1 = 0, r2 = 0, r3 = 0, r4 = 0;

vacuum full t;
vacuum analyze t;

set max_parallel_workers_per_gather = 0;

set work_mem = '2MB';

explain analyze
select * from t t1 join t t2 on (t1.a = t2.a)
 where t1.r1 + 0.0 < 1.0
   and t1.r2 + 0.0 < 1.0
   and t1.r3 + 0.0 < 1.0
   and t1.r4 + 0.0 < 1.0;

set work_mem = '1MB';

explain analyze
select * from t t1 join t t2 on (t1.a = t2.a)
 where t1.r1 + 0.0 < 1.0
   and t1.r2 + 0.0 < 1.0
   and t1.r3 + 0.0 < 1.0
   and t1.r4 + 0.0 < 1.0;

set work_mem = '256kB';

explain analyze
select * from t t1 join t t2 on (t1.a = t2.a)
 where t1.r1 + 0.0 < 1.0
   and t1.r2 + 0.0 < 1.0
   and t1.r3 + 0.0 < 1.0
   and t1.r4 + 0.0 < 1.0;
