\o /dev/null
CREATE COLUMN STORE ACCESS METHOD test HANDLER cstore_dummy_handler;
DROP TABLE IF EXISTS colstore_behave_test;
CREATE TABLE colstore_behave_test
(col1	bigint
,col2	text
,col3	bigint COLUMN STORE col3 USING test
,col4	integer
,col5	smallint
,col6	text
,col7	char(10) not null
,col8	text
);
\o

\i sql/colstore_behave.sql
