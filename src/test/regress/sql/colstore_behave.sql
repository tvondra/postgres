-- Required behavior for all column stores

INSERT INTO colstore_behave_test
VALUES (1, 'a', 12, 52, 42, 'col6a', 'col7z', 'col8_1234567');
INSERT INTO colstore_behave_test
VALUES (2, 'b', 22, 62, 82, 'col6b', 'col7y', 'col8_89101112');
INSERT INTO colstore_behave_test
VALUES (3, 'c', 32, 72, 92, 'col6c', 'col7x', 'col8_13141516888');

SELECT col1 FROM colstore_behave_test;
SELECT col2 FROM colstore_behave_test;
SELECT col3 FROM colstore_behave_test;
SELECT col4 FROM colstore_behave_test;
SELECT col5 FROM colstore_behave_test;
SELECT col6 FROM colstore_behave_test;
SELECT col7 FROM colstore_behave_test;
SELECT col8 FROM colstore_behave_test;

SELECT col1, col3, col6 FROM colstore_behave_test;
SELECT col6, col1, col3 FROM colstore_behave_test;

SELECT 1 FROM colstore_behave_test WHERE col1 >= 2;
SELECT 2 FROM colstore_behave_test WHERE col1 IN (1,3);
SELECT 3 FROM colstore_behave_test WHERE col2 = 'c';
SELECT 4 FROM colstore_behave_test WHERE col3 =12 OR col4 =62;
SELECT 5 FROM colstore_behave_test WHERE col6 LIKE 'col6%';
SELECT 6 FROM colstore_behave_test WHERE col7 LIKE '%7%';
SELECT 7 FROM colstore_behave_test WHERE col8 LIKE 'col8%';

SELECT col1 FROM colstore_behave_test WHERE col1 >= 2;
SELECT col1 FROM colstore_behave_test WHERE col1 IN (1,3);
SELECT col2 FROM colstore_behave_test WHERE col2 = 'c';
SELECT col3, col4 FROM colstore_behave_test WHERE col3 =12 OR col4 =62;
SELECT col6 FROM colstore_behave_test WHERE col6 LIKE 'col6%';
SELECT col7 FROM colstore_behave_test WHERE col7 LIKE '%7%';
SELECT col8 FROM colstore_behave_test WHERE col8 LIKE 'col8%';

SELECT * FROM colstore_behave_test WHERE col1 >= 2;
SELECT * FROM colstore_behave_test WHERE col1 IN (1,3);
SELECT * FROM colstore_behave_test WHERE col2 = 'c';
SELECT * FROM colstore_behave_test WHERE col3 =12 OR col4 =62;
SELECT * FROM colstore_behave_test WHERE col6 LIKE 'col6%';
SELECT * FROM colstore_behave_test WHERE col7 LIKE '%7%';
SELECT * FROM colstore_behave_test WHERE col8 LIKE 'col8%';


