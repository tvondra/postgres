drop table if exists t;

create table t (a int, b text);

copy t from '/tmp/hash-collisions.data' ;

-- make it larger than 1MB
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;

-- add a couple redundant columns
alter table t add column r1 double precision, add column r2 double precision, add column r3 double precision, add column r4 double precision;

update t set r1 = 0, r2 = 0, r3 = 0, r4 = 0;

vacuum full t;

insert into t select i, md5(i::text), r, r, r, r from (select i, random() as r from generate_series(1,1000000) s(i) where hashint4(i) > 1000000);

vacuum analyze t;

set max_parallel_workers_per_gather = 0;

set work_mem = '1MB';

explain analyze
select * from t t1 join t t2 on (t1.a = t2.a)
 where t1.r1 + 0.0 < 1.0
   and t1.r2 + 0.0 < 1.0
   and t1.r3 + 0.0 < 1.0
   and t1.r4 + 0.0 < 1.0;

set work_mem = '512kB';

explain analyze
select * from t t1 join t t2 on (t1.a = t2.a)
 where t1.r1 + 0.0 < 1.0
   and t1.r2 + 0.0 < 1.0
   and t1.r3 + 0.0 < 1.0
   and t1.r4 + 0.0 < 1.0;
