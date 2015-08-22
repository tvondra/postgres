DROP TABLE IF EXISTS colstore_behave_test;
CREATE TABLE colstore_behave_test
(col1	bigint
,col2	text
,col3	bigint
,col4	integer
,col5	smallint
,col6	text
,col7	char(10) not null
,col8	text
);

\i sql/colstore_behave.sql
