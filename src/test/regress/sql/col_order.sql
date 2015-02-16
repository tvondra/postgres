drop table if exists foo, bar, baz cascade;

create table foo (
	a int,
	b int,
	c int,
	d int,
	e int);
update pg_attribute set attphysnum = 4 where attname = 'b' and attrelid = 'foo'::regclass;
update pg_attribute set attphysnum = 2 where attname = 'd' and attrelid = 'foo'::regclass;
insert into foo values (1, 2, 3, 4, 5);
select * from foo;
-- \quit
drop table foo;

create table foo (
	a int default 42,
	b timestamp default '1975-02-15 12:00',
	c text);
insert into foo values (142857, '1888-04-29', 'hello world');

begin;
update pg_attribute set attlognum = 1 where attname = 'c' and attrelid = 'foo'::regclass;
update pg_attribute set attlognum = 2 where attname = 'a' and attrelid = 'foo'::regclass;
update pg_attribute set attlognum = 3 where attname = 'b' and attrelid = 'foo'::regclass;
commit;

insert into foo values ('column c', 123, '2010-03-03 10:10:10');
insert into foo (c, a, b) values ('c again', 456, '2010-03-03 11:12:13');
insert into foo values ('and c', 789);	-- defaults column b
insert into foo (c, b) values ('the c', '1975-01-10 08:00');	-- defaults column a

select * from foo;
select foo from foo;
select foo.* from foo;
select a,c,b from foo;
select c,b,a from foo;
select a from foo;
select b from foo;
select c from foo;
select (foo).* from foo;
select ROW((foo).*) from foo;
select ROW((foo).*)::foo from foo;
select (ROW((foo).*)::foo).* from foo;

create function f() returns setof foo language sql as $$
select * from foo;
$$;
select * from f();

insert into foo
	select (row('ah', 1126, '2012-10-15')::foo).*
	returning *;
insert into foo
	select (row('eh', 1125, '2012-10-16')::foo).*
	returning foo.*;

insert into foo values
	('values one', 1, '2008-10-20'),
	('values two', 2, '2004-08-15');

copy foo from stdin;
copy one	1001	1998-12-10 23:54
copy two	1002	1996-08-01 09:22
\.
select * from foo order by 2;

-- Test some joins
create table bar (x text, y int default 142857, z timestamp );
insert into bar values ('oh no', default, '1937-04-28');
insert into bar values ('oh yes', 42, '1492-12-31');
begin;
update pg_attribute set attlognum = 3 where attname = 'x' and attrelid = 'bar'::regclass;
update pg_attribute set attlognum = 1 where attname = 'z' and attrelid = 'bar'::regclass;
commit;
select foo.* from bar, foo where bar.y = foo.a;
select bar.* from bar, foo where bar.y = foo.a;
select * from bar, foo where bar.y = foo.a;
select * from foo join bar on (foo.a = bar.y);
alter table bar rename y to a;
select * from foo natural join bar;
select * from foo join bar using (a);

create table baz (e point) inherits (foo, bar); -- fail to merge defaults
create table baz (e point, a int default 23) inherits (foo, bar);
insert into baz (e) values ('(1,1)');
select * from foo;
select * from bar;
select * from baz;

create table quux (a int, b int[], c int);
begin;
update pg_attribute set attlognum = 1 where attnum = 2 and attrelid = 'quux'::regclass;
update pg_attribute set attlognum = 2 where attnum = 1 and attrelid = 'quux'::regclass;
commit;
select * from quux where (a,c) in ( select a,c from quux );


drop table foo, bar, baz, quux cascade;
