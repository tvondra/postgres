drop table if exists t;

create table t (a int, b text);

copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';
copy t from '/tmp/hash-collisions.data';

insert into t select i, md5(i::text) from generate_series(1,1000000) s(i);

create index on t (b);
cluster t USING t_b_idx;

vacuum analyze;

set hash_mem_multiplier = 1.0;

set work_mem = '2MB';

explain select * from t t1 join t t2 on (t1.a = t2.a);
explain analyze select * from t t1 join t t2 on (t1.a = t2.a);


set work_mem = '1MB';

explain select * from t t1 join t t2 on (t1.a = t2.a);
explain analyze select * from t t1 join t t2 on (t1.a = t2.a);


set work_mem = '512kB';

explain select * from t t1 join t t2 on (t1.a = t2.a);
explain analyze select * from t t1 join t t2 on (t1.a = t2.a);


set work_mem = '768kB';

explain select * from t t1 join t t2 on (t1.a = t2.a);
explain analyze select * from t t1 join t t2 on (t1.a = t2.a);
