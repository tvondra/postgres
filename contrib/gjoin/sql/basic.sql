drop extension if exists gjoin;
create extension gjoin;

drop table if exists t1;
drop table if exists t2;

create table t1 (a int, b int);
create table t2 (c int, d int);

insert into t1 select i, 1000 * random() from generate_series(1,10000) s(i);
insert into t2 select i, 1000 * random() from generate_series(1,10000) s(i);

vacuum analyze t1;
vacuum analyze t2;

explain select * from t1 join t2 on (t1.a = t2.c);

explain analyze select * from t1 join t2 on (t1.a = t2.c);

LOAD 'gjoin';
set gjoin.enabled = true;

select pg_backend_pid();
-- select pg_sleep(10);

explain select * from t1 join t2 on (t1.a = t2.c);

explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 100;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 200;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 500;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 1000;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 5000;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c) limit 10000;
explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.c);

-- explain (verbose, analyze) select * from t1 join t2 on (t1.a = t2.d) join t2 tx on (t2.c = tx.c);

-- select * from t1 join t2 on (t1.a = t2.c);
