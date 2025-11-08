create table dim1_1 (id int primary key, val2 text);
create table dim1_2 (id int primary key, val3 text);
create table dim1 (id int primary key,
                   id1_1 int references dim1_1(id),
                   id1_2 int references dim1_2(id),
                   val1 text);
create table dim2_1 (id int primary key, val5 text);
create table dim2_2 (id int primary key, val6 text);
create table dim2 (id int primary key,
                   id2_1 int references dim2_1(id),
                   id2_2 int references dim2_2(id),
                   val4 text);
create table dim3_1 (id int primary key, val5 text);
create table dim3_2 (id int primary key, val6 text);
create table dim3 (id int primary key,
                   id3_1 int references dim3_1(id),
                   id3_2 int references dim3_2(id),
                   val4 text);
create table dim4_1 (id int primary key, val5 text);
create table dim4_2 (id int primary key, val6 text);
create table dim4 (id int primary key,
                   id4_1 int references dim4_1(id),
                   id4_2 int references dim4_2(id),
                   val4 text);

create table t (id serial primary key,
                id1 int references dim1(id),
                id2 int references dim2(id),
                id3 int references dim3(id),
                id4 int references dim4(id));

vacuum analyze;
