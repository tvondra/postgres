create table dim1 (id int primary key, val1 text);
create table dim2 (id int primary key, val2 text);
create table dim3 (id int primary key, val3 text);
create table dim4 (id int primary key, val4 text);
create table dim5 (id int primary key, val5 text);
create table dim6 (id int primary key, val6 text);
create table dim7 (id int primary key, val7 text);

create table t (id serial primary key,
                id1 int references dim1(id),
                id2 int references dim1(id),
                id3 int references dim1(id),
                id4 int references dim1(id),
                id5 int references dim1(id),
                id6 int references dim1(id),
                id7 int references dim1(id));

vacuum analyze;
