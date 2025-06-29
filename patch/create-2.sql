create table t (id serial primary key, a text);

create table dim1 (id1 int primary key references t(id), val1 text);
create table dim2 (id2 int primary key references t(id), val2 text);
create table dim3 (id3 int primary key references t(id), val3 text);
create table dim4 (id4 int primary key references t(id), val4 text);
create table dim5 (id5 int primary key references t(id), val5 text);
create table dim6 (id6 int primary key references t(id), val6 text);
create table dim7 (id7 int primary key references t(id), val7 text);

vacuum analyze;
