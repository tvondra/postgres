-- JSON_EXISTS
SELECT JSON_EXISTS(NULL::jsonb, '$');
SELECT JSON_EXISTS(jsonb '[]', '$');
SELECT JSON_EXISTS(JSON_OBJECT(RETURNING jsonb), '$');

SELECT JSON_EXISTS(jsonb '1', '$');
SELECT JSON_EXISTS(jsonb 'null', '$');
SELECT JSON_EXISTS(jsonb '[]', '$');

SELECT JSON_EXISTS(jsonb '1', '$.a');
SELECT JSON_EXISTS(jsonb '1', 'strict $.a'); -- FALSE on error
SELECT JSON_EXISTS(jsonb '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_EXISTS(jsonb 'null', '$.a');
SELECT JSON_EXISTS(jsonb '[]', '$.a');
SELECT JSON_EXISTS(jsonb '[1, "aaa", {"a": 1}]', 'strict $.a'); -- FALSE on error
SELECT JSON_EXISTS(jsonb '[1, "aaa", {"a": 1}]', 'lax $.a');
SELECT JSON_EXISTS(jsonb '{}', '$.a');
SELECT JSON_EXISTS(jsonb '{"b": 1, "a": 2}', '$.a');

SELECT JSON_EXISTS(jsonb '1', '$.a.b');
SELECT JSON_EXISTS(jsonb '{"a": {"b": 1}}', '$.a.b');
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.a.b');

SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING 1 AS x);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING '1' AS x);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 2 AS y);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 1 AS y);

-- extension: boolean expressions
SELECT JSON_EXISTS(jsonb '1', '$ > 2');
SELECT JSON_EXISTS(jsonb '1', '$.a > 2' ERROR ON ERROR);

-- JSON_VALUE
SELECT JSON_VALUE(NULL::jsonb, '$');

SELECT JSON_VALUE(jsonb 'null', '$');
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING int);

SELECT JSON_VALUE(jsonb 'true', '$');
SELECT JSON_VALUE(jsonb 'true', '$' RETURNING bool);

SELECT JSON_VALUE(jsonb '123', '$');
SELECT JSON_VALUE(jsonb '123', '$' RETURNING int) + 234;
SELECT JSON_VALUE(jsonb '123', '$' RETURNING text);
/* jsonb bytea ??? */
SELECT JSON_VALUE(jsonb '123', '$' RETURNING bytea ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '1.23', '$');
SELECT JSON_VALUE(jsonb '1.23', '$' RETURNING int);
SELECT JSON_VALUE(jsonb '"1.23"', '$' RETURNING numeric);
SELECT JSON_VALUE(jsonb '"1.23"', '$' RETURNING int ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '"aaa"', '$');
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING text);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(5));
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(2) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(2));
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(3) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING json);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING jsonb);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING json ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING jsonb ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"\"aaa\""', '$' RETURNING json);
SELECT JSON_VALUE(jsonb '"\"aaa\""', '$' RETURNING jsonb);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int DEFAULT 111 ON ERROR);
SELECT JSON_VALUE(jsonb '"123"', '$' RETURNING int) + 234;

SELECT JSON_VALUE(jsonb '"2017-02-20"', '$' RETURNING date) + 9;

-- Test NULL checks execution in domain types
CREATE DOMAIN sqljsonb_int_not_null AS int NOT NULL;
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null);
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null ERROR ON ERROR);
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null DEFAULT 2 ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1',  '$.a' RETURNING sqljsonb_int_not_null DEFAULT 2 ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1',  '$.a' RETURNING sqljsonb_int_not_null DEFAULT NULL ON EMPTY ERROR ON ERROR);
CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow', 'green', 'blue', 'purple');
CREATE DOMAIN rgb AS rainbow CHECK (VALUE IN ('red', 'green', 'blue'));
SELECT JSON_VALUE('"purple"'::jsonb, 'lax $[*]' RETURNING rgb);
SELECT JSON_VALUE('"purple"'::jsonb, 'lax $[*]' RETURNING rgb ERROR ON ERROR);
SELECT x IS NULL AS is_null,
       json_value(x, 'strict $.a' RETURNING sqljsonb_int_not_null DEFAULT 1 ON ERROR) AS jv
FROM (VALUES ('1'::jsonb), (NULL), ('1'::jsonb)) v(x);

SELECT JSON_VALUE(jsonb '[]', '$');
SELECT JSON_VALUE(jsonb '[]', '$' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '{}', '$');
SELECT JSON_VALUE(jsonb '{}', '$' ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '1', '$.a');
SELECT JSON_VALUE(jsonb '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'strict $.a' DEFAULT 'error' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON ERROR);	-- NULL ON EMPTY
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'strict $.*' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT '2' ON EMPTY);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' NULL ON EMPTY DEFAULT '2' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT '2' ON EMPTY DEFAULT '3' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON EMPTY DEFAULT '3' ON ERROR);

SELECT JSON_VALUE(jsonb '[1,2]', '$[*]' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '[1,2]', '$[*]' DEFAULT '0' ON ERROR);
SELECT JSON_VALUE(jsonb '[" "]', '$[*]' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '[" "]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING int FORMAT JSON); -- RETURNING FORMAT not allowed

-- A DEFAULT expression must be coerced to the RETURNING type's typmod even
-- when its base type already matches, but a matching NULL needs no coercion.
SELECT JSON_VALUE(jsonb '{}', '$.a' RETURNING numeric(4,1) DEFAULT 99999.999 ON EMPTY);
SELECT JSON_VALUE(jsonb '{}', '$.a' RETURNING varchar(3) DEFAULT 'toolong'::varchar(10) ON EMPTY);
SELECT JSON_VALUE(jsonb '{}', '$.a' RETURNING numeric(4,1) DEFAULT NULL::numeric ON EMPTY);
SELECT JSON_VALUE(jsonb '{}', '$.a' RETURNING bit(3) DEFAULT b'10101' ON EMPTY);
SELECT JSON_VALUE(jsonb '{}', '$.a' RETURNING numeric(4,1) DEFAULT abs(NULL::numeric) ON EMPTY);

-- RETUGNING pseudo-types not allowed
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING record);

SELECT
	x,
	JSON_VALUE(
		jsonb '{"a": 1, "b": 2}',
		'$.* ? (@ > $x)' PASSING x AS x
		RETURNING int
		DEFAULT -1 ON EMPTY
		DEFAULT -2 ON ERROR
	) y
FROM
	generate_series(0, 2) x;

-- Check that a NULL context item or path doesn't return a previous row's
-- ON EMPTY / ON ERROR result (bug #19621).
SELECT x IS NULL AS is_null,
       json_value(x, '$.a' RETURNING int DEFAULT 42 ON EMPTY) AS jv
FROM (VALUES ('{}'), (NULL), ('{}')) v(x);

SELECT x IS NULL AS is_null,
       json_value(x, 'strict $.a' RETURNING int DEFAULT 42 ON ERROR) AS jv
FROM (VALUES ('1'), (NULL), ('1')) v(x);

SELECT p IS NULL AS is_null,
       json_value('{}', p RETURNING int DEFAULT 42 ON EMPTY) AS jv
FROM (VALUES ('$.a'::jsonpath), (NULL), ('$.a'::jsonpath)) v(p);

SELECT x IS NULL AS is_null,
       json_query(x, '$.a' DEFAULT '"empty"' ON EMPTY) AS jq
FROM (VALUES ('{}'::jsonb), (NULL), ('{}'::jsonb)) v(x);

SELECT x IS NULL AS is_null,
       json_exists(x, 'strict $.a' TRUE ON ERROR) AS je
FROM (VALUES ('1'::jsonb), (NULL), ('1'::jsonb)) v(x);
-- the default FALSE ON ERROR is affected too
SELECT x IS NULL AS is_null,
       json_exists(x, 'strict $.a') AS je
FROM (VALUES ('1'::jsonb), (NULL), ('1'::jsonb)) v(x);

SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a);
SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a RETURNING point);
SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a RETURNING point ERROR ON ERROR);

-- Test PASSING and RETURNING date/time types
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING timestamptz);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING timestamp);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING date '2018-02-21 12:34:56 +10' AS ts RETURNING date);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING time '2018-02-21 12:34:56 +10' AS ts RETURNING time);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timetz '2018-02-21 12:34:56 +10' AS ts RETURNING timetz);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamp '2018-02-21 12:34:56 +10' AS ts RETURNING timestamp);

-- Also test RETURNING json[b]
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING json);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING jsonb);

-- Test that numeric JSON values are coerced uniformly
select json_value('{"a": 1.234}', '$.a' returning int error on error);
select json_value('{"a": "1.234"}', '$.a' returning int error on error);

-- JSON_QUERY

SELECT JSON_VALUE(NULL::jsonb, '$');

SELECT
	JSON_QUERY(js, '$') AS "unspec",
	JSON_QUERY(js, '$' WITHOUT WRAPPER) AS "without",
	JSON_QUERY(js, '$' WITH CONDITIONAL WRAPPER) AS "with cond",
	JSON_QUERY(js, '$' WITH UNCONDITIONAL ARRAY WRAPPER) AS "with uncond",
	JSON_QUERY(js, '$' WITH ARRAY WRAPPER) AS "with"
FROM
	(VALUES
		(jsonb 'null'),
		('12.3'),
		('true'),
		('"aaa"'),
		('[1, null, "2"]'),
		('{"a": 1, "b": [2]}')
	) foo(js);

SELECT
	JSON_QUERY(js, 'strict $[*]') AS "unspec",
	JSON_QUERY(js, 'strict $[*]' WITHOUT WRAPPER) AS "without",
	JSON_QUERY(js, 'strict $[*]' WITH CONDITIONAL WRAPPER) AS "with cond",
	JSON_QUERY(js, 'strict $[*]' WITH UNCONDITIONAL ARRAY WRAPPER) AS "with uncond",
	JSON_QUERY(js, 'strict $[*]' WITH ARRAY WRAPPER) AS "with"
FROM
	(VALUES
		(jsonb '1'),
		('[]'),
		('[null]'),
		('[12.3]'),
		('[true]'),
		('["aaa"]'),
		('[[1, 2, 3]]'),
		('[{"a": 1, "b": [2]}]'),
		('[1, "2", null, [3]]')
	) foo(js);

SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text KEEP QUOTES);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text KEEP QUOTES ON SCALAR STRING);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text OMIT QUOTES);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text OMIT QUOTES ON SCALAR STRING);
SELECT JSON_QUERY(jsonb '"aaa"', '$' OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING json OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING bytea FORMAT JSON OMIT QUOTES ERROR ON ERROR);

-- Behavior when a RETURNING type has typmod != -1
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3) ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3));
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3) OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$.a' RETURNING char(2) OMIT QUOTES DEFAULT 'bb' ON EMPTY);
SELECT JSON_QUERY(jsonb '"aaa"', '$.a' RETURNING char(2) OMIT QUOTES DEFAULT '"bb"'::jsonb ON EMPTY);

-- OMIT QUOTES behavior should not be specified when WITH WRAPPER used:
-- Should fail
SELECT JSON_QUERY(jsonb '[1]', '$' WITH WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(jsonb '[1]', '$' WITH CONDITIONAL WRAPPER OMIT QUOTES);
-- Should succeed
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH CONDITIONAL WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH UNCONDITIONAL WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITHOUT WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITHOUT WRAPPER KEEP QUOTES);

-- test QUOTES behavior.
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] keep quotes error on error);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' returning int4range omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' returning int4range keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' 	returning int4range keep quotes error on error);
CREATE DOMAIN qf_char_domain AS char(1);
CREATE DOMAIN qf_jsonb_domain AS jsonb;
SELECT JSON_QUERY(jsonb '"1"', '$' RETURNING qf_char_domain OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"1"', '$' RETURNING qf_jsonb_domain OMIT QUOTES ERROR ON ERROR);
DROP DOMAIN qf_char_domain, qf_jsonb_domain;

SELECT JSON_QUERY(jsonb '[]', '$[*]');
SELECT JSON_QUERY(jsonb '[]', '$[*]' NULL ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY ARRAY ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY OBJECT ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' DEFAULT '"empty"' ON EMPTY);

SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY NULL ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY EMPTY ARRAY ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON ERROR);	-- NULL ON EMPTY

SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' DEFAULT '"empty"' ON ERROR);

SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING json);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING json FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING jsonb);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING jsonb FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING text);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING char(10));
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING text FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING bytea);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING bytea FORMAT JSON);

SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING bytea EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING bytea FORMAT JSON EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING json EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING jsonb EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[3,4]', '$[*]' RETURNING bigint[] EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '"[3,4]"', '$[*]' RETURNING bigint[] EMPTY OBJECT ON ERROR);

-- Coercion fails with quotes on
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int2 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int4 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int8 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING bool error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING numeric error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING real error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING float8 error on error);
-- Fine with OMIT QUOTES
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int2 omit quotes error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING float8 omit quotes error on error);

-- RETUGNING pseudo-types not allowed
SELECT JSON_QUERY(jsonb '[3,4]', '$[*]' RETURNING anyarray EMPTY OBJECT ON ERROR);

SELECT
	x, y,
	JSON_QUERY(
		jsonb '[1,2,3,4,5,null]',
		'$[*] ? (@ >= $x && @ <= $y)'
		PASSING x AS x, y AS y
		WITH CONDITIONAL WRAPPER
		EMPTY ARRAY ON EMPTY
	) list
FROM
	generate_series(0, 4) x,
	generate_series(0, 4) y;

-- record type returning with quotes behavior.
CREATE TYPE comp_abc AS (a text, b int, c timestamp);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc keep quotes error on error);
DROP TYPE comp_abc;

-- Extension: record types returning
CREATE TYPE sqljsonb_rec AS (a int, t text, js json, jb jsonb, jsa json[]);
CREATE TYPE sqljsonb_reca AS (reca sqljsonb_rec[]);

SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec);
SELECT JSON_QUERY(jsonb '[{"a": "a", "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[{"a": "a", "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec);
SELECT * FROM unnest((JSON_QUERY(jsonb '{"jsa":  [{"a": 1, "b": ["foo"]}, {"a": 2, "c": {}}, 123]}', '$' RETURNING sqljsonb_rec)).jsa);
SELECT * FROM unnest((JSON_QUERY(jsonb '{"reca": [{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]}', '$' RETURNING sqljsonb_reca)).reca);

SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING jsonpath);
SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING jsonpath ERROR ON ERROR);

-- Extension: array types returning
SELECT JSON_QUERY(jsonb '[1,2,null,"3"]', '$[*]' RETURNING int[] WITH WRAPPER);
SELECT JSON_QUERY(jsonb '[1,2,null,"a"]', '$[*]' RETURNING int[] WITH WRAPPER ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2,null,"a"]', '$[*]' RETURNING int[] WITH WRAPPER);
SELECT * FROM unnest(JSON_QUERY(jsonb '[{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]', '$' RETURNING sqljsonb_rec[]));

-- Extension: domain types returning
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.a' RETURNING sqljsonb_int_not_null);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' RETURNING sqljsonb_int_not_null);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' RETURNING sqljsonb_int_not_null ERROR ON EMPTY ERROR ON ERROR);

-- Test timestamptz passing and output
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts);
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING json);
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING jsonb);

-- Test constraints

CREATE TABLE test_jsonb_constraints (
	js text,
	i int,
	x jsonb DEFAULT JSON_QUERY(jsonb '[1,2]', '$[*]' WITH WRAPPER)
	CONSTRAINT test_jsonb_constraint1
		CHECK (js IS JSON)
	CONSTRAINT test_jsonb_constraint2
		CHECK (JSON_EXISTS(js::jsonb, '$.a' PASSING i + 5 AS int, i::text AS "TXT", array[1,2,3] as arr))
	CONSTRAINT test_jsonb_constraint3
		CHECK (JSON_VALUE(js::jsonb, '$.a' RETURNING int DEFAULT '12' ON EMPTY ERROR ON ERROR) > i)
	CONSTRAINT test_jsonb_constraint4
		CHECK (JSON_QUERY(js::jsonb, '$.a' WITH CONDITIONAL WRAPPER EMPTY OBJECT ON ERROR) = jsonb '[10]')
	CONSTRAINT test_jsonb_constraint5
		CHECK (JSON_QUERY(js::jsonb, '$.a' RETURNING char(5) OMIT QUOTES EMPTY ARRAY ON EMPTY) >  'a' COLLATE "C")
);

\d test_jsonb_constraints

SELECT check_clause
FROM information_schema.check_constraints
WHERE constraint_name LIKE 'test_jsonb_constraint%'
ORDER BY 1;

SELECT pg_get_expr(adbin, adrelid)
FROM pg_attrdef
WHERE adrelid = 'test_jsonb_constraints'::regclass
ORDER BY 1;

INSERT INTO test_jsonb_constraints VALUES ('', 1);
INSERT INTO test_jsonb_constraints VALUES ('1', 1);
INSERT INTO test_jsonb_constraints VALUES ('[]');
INSERT INTO test_jsonb_constraints VALUES ('{"b": 1}', 1);
INSERT INTO test_jsonb_constraints VALUES ('{"a": 1}', 1);
INSERT INTO test_jsonb_constraints VALUES ('{"a": 10}', 1);

DROP TABLE test_jsonb_constraints;

-- Test mutability of query functions
CREATE TABLE test_jsonb_mutability(js jsonb, b int);
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a[0]'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.time()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.time_tz()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.timestamp()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.timestamp_tz()'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.time_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.time())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.time())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.time_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.timestamp_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.timestamp_tz())'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.datetime("HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.datetime("HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date() < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date() < $x' PASSING '1234'::int AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp(2) < $.timestamp(3))'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@ < $.datetime())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime() < $.datetime())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI TZH") < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI") < $.datetime("YY-MM-DD HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI TZH") < $.datetime("YY-MM-DD HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("HH:MI TZH") < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("HH:MI TZH") < $y' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() < $x' PASSING '1234'::int AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() ? (@ == $x)' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("YY-MM-DD") ? (@ == $x)' PASSING '2020-07-14'::date AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, $.a ? (@.datetime() == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, 0 to $.a ? (@.datetime() == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, $.a ? (@.datetime("HH:MI") == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_VALUE(js, '$' DEFAULT random()::int ON ERROR));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.rtrim()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.ltrim()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.btrim()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.lower()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.upper()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.initcap()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.replace("hello", "bye")'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.split_part(",", 2)'));

-- DEFAULT expression
CREATE OR REPLACE FUNCTION ret_setint() RETURNS SETOF integer AS
$$
BEGIN
    RETURN QUERY EXECUTE 'select 1 union all select 1';
END;
$$
LANGUAGE plpgsql IMMUTABLE;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT ret_setint() ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT b + 1 ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT sum(1) over() ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT (SELECT 1) ON ERROR) FROM test_jsonb_mutability;
DROP TABLE test_jsonb_mutability;
DROP FUNCTION ret_setint;

CREATE DOMAIN queryfuncs_test_domain AS text CHECK (value <> 'foo');
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo'::queryfuncs_test_domain ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo1'::queryfuncs_test_domain ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT '"foo1"'::jsonb::text ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "foo"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo1'::queryfuncs_test_domain ON EMPTY);

-- Check the cases where a coercion-related expression is masking an
-- unsupported expressions

-- CoerceViaIO
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int DEFAULT (SELECT '"1"')::jsonb ON ERROR);
-- CoerceToDomain
SELECT JSON_QUERY('"a"', '$.a' RETURNING queryfuncs_test_domain DEFAULT (select '"1"')::queryfuncs_test_domain ON ERROR);
-- RelabelType
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int DEFAULT (SELECT 1)::oid::int ON ERROR);
-- ArrayCoerceExpr
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int[] DEFAULT (SELECT '{1}')::oid[]::int[] ON ERROR);
-- CollateExpr
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int[] DEFAULT (SELECT '{1}')::text COLLATE "C" ON ERROR);
-- ConvertRowtypeExpr
CREATE TABLE someparent (a int);
CREATE TABLE somechild () INHERITS (someparent);
SELECT JSON_QUERY('"a"', '$.a'  RETURNING someparent DEFAULT (SELECT '(1)')::somechild::someparent ON ERROR);

DROP DOMAIN queryfuncs_test_domain;
DROP TABLE someparent, somechild;

-- Extension: non-constant JSON path
SELECT JSON_EXISTS(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_VALUE(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_VALUE(jsonb '{"a": 123}', '$' || '.' || 'b' DEFAULT 'foo' ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_QUERY(jsonb '{"a": 123}', '$' || '.' || 'a' WITH WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": 123}', ('$' || '.' || 'a' || NULL)::date WITH WRAPPER);
-- Should fail (invalid path)
SELECT JSON_QUERY(jsonb '{"a": 123}', 'error' || ' ' || 'error');

-- Non-jsonb inputs automatically coerced to jsonb
SELECT JSON_EXISTS(json '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_QUERY(NULL FORMAT JSON, '$');

-- Test non-const jsonpath
CREATE TEMP TABLE jsonpaths (path) AS SELECT '$';
SELECT json_value('"aaa"', path RETURNING json) FROM jsonpaths;
SELECT json_value('"aaa"', jsonpaths RETURNING json) FROM jsonpaths;

-- Test PASSING argument parsing
SELECT JSON_QUERY(jsonb 'null', '$xyz' PASSING 1 AS xy);
SELECT JSON_QUERY(jsonb 'null', '$xy' PASSING 1 AS xyz);
SELECT JSON_QUERY(jsonb 'null', '$xyz' PASSING 1 AS xyz);
SELECT JSON_QUERY(jsonb 'null', '$Xyz' PASSING 1 AS Xyz);
SELECT JSON_QUERY(jsonb 'null', '$Xyz' PASSING 1 AS "Xyz");
SELECT JSON_QUERY(jsonb 'null', '$"Xyz"' PASSING 1 AS "Xyz");

-- Test PASSING and toasted values
CREATE TABLE test_passing_toast (t text);
ALTER TABLE test_passing_toast ALTER COLUMN t SET STORAGE EXTENDED;
INSERT INTO test_passing_toast(t)
  SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50)
  FROM generate_series(1, 500) g(i);
ALTER TABLE test_passing_toast ALTER COLUMN t SET STORAGE EXTERNAL;
INSERT INTO test_passing_toast(t)
  SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50)
  FROM generate_series(1, 500) g(i);
SELECT count(*) AS toasted_chunks
  FROM test_passing_toast WHERE pg_column_toast_chunk_id(t) IS NOT NULL;
SELECT JSON_VALUE(jsonb 'null', '$a' PASSING t AS a) = t AS ok
  FROM test_passing_toast;
DROP TABLE test_passing_toast;

-- Test PASSING of string types other than text and varchar; they are
-- converted the same way as by to_jsonb(), so char(n) keeps its padding
CREATE DOMAIN queryfuncs_text AS text;
CREATE DOMAIN queryfuncs_char3 AS char(3);
SELECT JSON_EXISTS(jsonb '["abc"]', '$[*] ? (@ == $c)' PASSING 'abc'::char(3) AS c);
SELECT JSON_QUERY(jsonb 'null', '$c' PASSING 'ab'::char(3) AS c),
       to_jsonb('ab'::char(3));
SELECT JSON_VALUE(jsonb 'null', '$c' PASSING 'abc'::name AS c);
SELECT JSON_QUERY(jsonb 'null', '$c' PASSING 'abc'::queryfuncs_text AS c);
SELECT JSON_QUERY(jsonb 'null', '$c' PASSING 'ab'::queryfuncs_char3 AS c);
SELECT * FROM JSON_TABLE(jsonb '[1]', '$[*]' PASSING 'abc'::char(3) AS c
  COLUMNS (v text PATH '$c'));
DROP DOMAIN queryfuncs_text;
DROP DOMAIN queryfuncs_char3;
-- ... including a non-built-in string type, which uses its output function
CREATE TYPE queryfuncs_str;
CREATE FUNCTION queryfuncs_str_in(cstring) RETURNS queryfuncs_str
  AS 'textin' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION queryfuncs_str_out(queryfuncs_str) RETURNS cstring
  AS 'textout' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE queryfuncs_str (INPUT = queryfuncs_str_in, OUTPUT = queryfuncs_str_out,
  LIKE = text, CATEGORY = 'S');
SELECT JSON_QUERY(jsonb 'null', '$c' PASSING 'abc'::queryfuncs_str AS c);
DROP TYPE queryfuncs_str CASCADE;

-- Test ON ERROR / EMPTY value validity for the function; all fail.
SELECT JSON_EXISTS(jsonb '1', '$' DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '1', '$' EMPTY ON ERROR);
SELECT JSON_QUERY(jsonb '1', '$' TRUE ON ERROR);

-- soft errors raised by the ON ERROR or ON EMPTY DEFAULT expression must always
-- be rethrown
SELECT JSON_VALUE('"a"', '$' RETURNING sqljsonb_int_not_null DEFAULT NULL ON ERROR);
SELECT JSON_VALUE('"a"', '$' RETURNING int DEFAULT (NULL::sqljsonb_int_not_null + 0) ON ERROR);
SELECT JSON_QUERY('"a"', '$' RETURNING int DEFAULT (NULL::sqljsonb_int_not_null + 0) ON ERROR);
SELECT JSON_VALUE('"a"', '$' RETURNING int DEFAULT (('a' || (random() * 0)::int)::int + 0) ON ERROR);
SELECT JSON_QUERY('"a"', '$' RETURNING int DEFAULT (('a' || (random() * 0)::int)::int + 0) ON ERROR);
-- error. should report that the DEFAULT expression cannot be coerced to the
-- RETURNING type, not a NOT NULL violation
SELECT JSON_VALUE(jsonb '{"a": 1}', '$' RETURNING sqljsonb_int_not_null DEFAULT concat('y') ON ERROR);

-- domain constraints must not be checked once a soft error has already occurred
CREATE DOMAIN queryfuncs_int AS int CHECK (1 / coalesce(VALUE, 0) = 0);
SELECT JSON_VALUE(jsonb '"x"', '$' RETURNING int DEFAULT 11 ON ERROR);
SELECT JSON_QUERY(jsonb '["x"]', '$' RETURNING int[] DEFAULT '{11}' ON ERROR);
SELECT JSON_VALUE(jsonb '"x"', '$' RETURNING queryfuncs_int DEFAULT 11 ON ERROR);
SELECT JSON_QUERY(jsonb '["x"]', '$' RETURNING queryfuncs_int[] DEFAULT '{11}' ON ERROR);

CREATE TYPE compcoalesce AS (a int, b queryfuncs_int);
SELECT JSON_QUERY(jsonb '{"a": "x"}', '$' RETURNING compcoalesce);
SELECT JSON_QUERY(jsonb '{"a": 1, "b": 11}', '$' RETURNING compcoalesce);
SELECT JSON_QUERY(jsonb '{"a": 1, "b": "B"}', '$' RETURNING compcoalesce);
SELECT JSON_QUERY(jsonb '{"a": 1, "b": 0}', '$' RETURNING compcoalesce); -- error
DROP TYPE compcoalesce;
DROP DOMAIN queryfuncs_int;

-- Test implicit coercion to a domain over fixed-length type specified in
-- RETURNING
CREATE DOMAIN queryfuncs_char2 AS char(2);
CREATE DOMAIN queryfuncs_char2_chk AS char(2) CHECK (VALUE NOT IN ('12'));
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2 ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2 DEFAULT '1' ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2_chk ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2_chk DEFAULT '1' ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2 ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2 DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2_chk ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2_chk DEFAULT 1 ON ERROR);
DROP DOMAIN queryfuncs_char2, queryfuncs_char2_chk;

-- Test coercion to domain over another fixed-length type of the ON ERROR /
-- EMPTY expressions.  Ask user to cast the DEFAULT expression explicitly if
-- automatic casting cannot be done, for example, from int to bit(2).
CREATE DOMAIN queryfuncs_d_varbit3 AS varbit(3) CHECK (VALUE <> '01');
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '111111' ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '010' ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '01' ON ERROR);
SELECT JSON_VALUE(jsonb '"111"', '$'  RETURNING bit(2) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING bit(3)  DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING bit(3)  DEFAULT 1::bit(3) ON ERROR);
SELECT JSON_VALUE(jsonb '"111"', '$.a'  RETURNING bit(3) DEFAULT '1111' ON EMPTY);
DROP DOMAIN queryfuncs_d_varbit3;

-- Check that a NULL result from a previous evaluation of JSON_VALUE()
-- RETURNING json or jsonb doesn't leak into the next one (bug #19695).
SELECT JSON_VALUE('null', '$' RETURNING jsonb), JSON_VALUE('123', '$' RETURNING jsonb);
SELECT JSON_VALUE(x, '$' RETURNING jsonb) FROM (VALUES ('1'::jsonb), ('null'), ('2')) v(x);
SELECT JSON_VALUE(x, '$' RETURNING json) FROM (VALUES ('1'::json), ('null'), ('2')) v(x);
-- same, with the NULL coming from ON EMPTY, ON ERROR, and a NULL input
SELECT JSON_VALUE(x, '$.a' RETURNING jsonb) FROM (VALUES ('{"a": 1}'::jsonb), ('{}'), ('{"a": 2}')) v(x);
SELECT JSON_VALUE(x, 'strict $.a' RETURNING jsonb) FROM (VALUES ('{"a": 1}'::jsonb), ('[]'), ('{"a": 2}')) v(x);
SELECT JSON_VALUE(x, '$' RETURNING jsonb) FROM (VALUES ('1'::jsonb), (NULL), ('2')) v(x);

-- Non-strict context and path expressions need null-extension protection.
SELECT a.x, b.context_expr, b.path_expr
FROM (VALUES (0), (1)) AS a(x)
LEFT JOIN (
  SELECT v.x,
         JSON_VALUE(COALESCE(v.js, '0'::jsonb), '$' RETURNING int) AS context_expr,
         JSON_VALUE('0'::jsonb, COALESCE(v.p, '$'::jsonpath)
                    RETURNING int) AS path_expr
  FROM (VALUES (0, '0'::jsonb, '$'::jsonpath), (2, '2', '$')) AS v(x, js, p)
) AS b ON a.x = b.x
ORDER BY a.x;

-- A constant-valued PlaceHolderVar in PASSING still counts as a nullable
-- input; inspecting only its expression would miss null-extension.
SELECT a.x, b.j
FROM (VALUES (0), (1)) AS a(x)
LEFT JOIN (
  SELECT v.x, JSON_VALUE('0'::jsonb, '$' PASSING p.k AS p RETURNING int) AS j
  FROM (VALUES (0), (2)) AS v(x)
  LEFT JOIN (SELECT x, 1 AS k FROM (VALUES (0), (2)) AS w(x)) AS p USING (x)
) AS b ON a.x = b.x
ORDER BY a.x;

-- Constant PASSING and default inputs must not prevent inlining when the
-- function's parameter is used as the context item or path specification.
CREATE FUNCTION sqljson_strict_context(jsonb) RETURNS int
LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT JSON_VALUE($1, '$.a' PASSING 1 AS p
                    RETURNING int DEFAULT 0 ON EMPTY DEFAULT -1 ON ERROR)
$$;
CREATE FUNCTION sqljson_strict_path(jsonpath) RETURNS int
LANGUAGE sql STABLE STRICT AS $$
  SELECT JSON_VALUE('{"a": 1}'::jsonb, $1
                    RETURNING int DEFAULT 0 ON EMPTY DEFAULT -1 ON ERROR)
$$;

-- Verify inlining with constant PASSING arguments and default values.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sqljson_strict_context(js), sqljson_strict_path(p)
FROM (VALUES (NULL::jsonb, NULL::jsonpath),
             ('{}', '$.missing'),
             ('{"a": 1}', '$.a'),
             ('{"a": "bad"}', 'strict $.missing')) AS v(js, p);

-- Check NULL propagation and successful, ON EMPTY, and ON ERROR results.
SELECT sqljson_strict_context(js), sqljson_strict_path(p)
FROM (VALUES (NULL::jsonb, NULL::jsonpath),
             ('{}', '$.missing'),
             ('{"a": 1}', '$.a'),
             ('{"a": "bad"}', 'strict $.missing')) AS v(js, p);

DROP FUNCTION sqljson_strict_context(jsonb), sqljson_strict_path(jsonpath);

-- Wrappers and constant behavior clauses do not prevent strictness in the
-- context item and path specification for JSON_QUERY and JSON_EXISTS either.
CREATE FUNCTION sqljson_query_strict(jsonb, jsonpath) RETURNS jsonb
LANGUAGE sql STABLE STRICT AS $$
  SELECT JSON_QUERY($1, $2 PASSING 0 AS p RETURNING jsonb WITH ARRAY WRAPPER
                    EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)
$$;
CREATE FUNCTION sqljson_exists_strict(jsonb, jsonpath) RETURNS bool
LANGUAGE sql STABLE STRICT AS $$
  SELECT JSON_EXISTS($1, $2 PASSING 0 AS p TRUE ON ERROR)
$$;

-- Wrappers and constant behavior clauses must not prevent inlining.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sqljson_query_strict(js, p), sqljson_exists_strict(js, p)
FROM (VALUES (NULL::jsonb, '$'::jsonpath),
             ('0', NULL),
             ('[]', '$[*]'),
             ('[1]', '$[*] ? (@ > $p)'),
             ('{}', 'strict $.missing'),
             ('null', '$')) AS v(js, p);

-- Check NULL inputs, matches, empty/error behavior, and wrapped JSON null.
SELECT sqljson_query_strict(js, p), sqljson_exists_strict(js, p)
FROM (VALUES (NULL::jsonb, '$'::jsonpath),
             ('0', NULL),
             ('[]', '$[*]'),
             ('[1]', '$[*] ? (@ > $p)'),
             ('{}', 'strict $.missing'),
             ('null', '$')) AS v(js, p);

DROP FUNCTION sqljson_query_strict(jsonb, jsonpath),
              sqljson_exists_strict(jsonb, jsonpath);

-- A NULL PASSING argument or DEFAULT expression doesn't force a NULL result,
-- so the planner must not consider them strict.  When a subquery is pulled up
-- from the nullable side of an outer join, its output expressions must be
-- evaluated below the join.
-- the PASSING value is not referenced by the path, and the DEFAULT expression
-- is not evaluated as the path yields an item, so the results are not NULL
SELECT JSON_VALUE(jsonb '1', '$' PASSING NULL::int AS v RETURNING int),
       JSON_VALUE(jsonb '{"a": 1}', '$.a' RETURNING int DEFAULT NULL ON EMPTY);
-- the same for JSON_QUERY() and JSON_EXISTS(), for which a NULL PASSING value
-- is a JSON null even if the path references it
SELECT JSON_QUERY(jsonb '[1]', '$' PASSING NULL::int AS v),
       JSON_EXISTS(jsonb '1', '$ ? (@ == $v)' PASSING NULL::int AS v),
       JSON_QUERY(jsonb '{"a": [1]}', '$.a' DEFAULT NULL ON ERROR);
-- pull-up with a PASSING argument: JSON_VALUE() is computed below the join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_VALUE(jsonb '1', '$' PASSING y AS v RETURNING int) AS jv
                FROM (VALUES (2), (3)) b(y)) ss ON x = y
ORDER BY x;
-- so jv is NULL for the unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_VALUE(jsonb '1', '$' PASSING y AS v RETURNING int) AS jv
                FROM (VALUES (2), (3)) b(y)) ss ON x = y
ORDER BY x;
-- OFFSET 0 prevents the pull-up, so jv is NULL for the unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_VALUE(jsonb '1', '$' PASSING y AS v RETURNING int) AS jv
                FROM (VALUES (2), (3)) b(y) OFFSET 0) ss ON x = y
ORDER BY x;
-- JSON_QUERY() and JSON_EXISTS() with PASSING are computed below the join too
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, y, jq, je
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_QUERY(jsonb '[1]', '$' PASSING y AS v) AS jq,
                       JSON_EXISTS(jsonb '2', '$ ? (@ == $v)' PASSING y AS v) AS je
                FROM (VALUES (2), (3)) b(y)) ss ON x = y
ORDER BY x;
-- so jq and je are NULL for the unmatched x = 1
SELECT x, y, jq, je
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_QUERY(jsonb '[1]', '$' PASSING y AS v) AS jq,
                       JSON_EXISTS(jsonb '2', '$ ? (@ == $v)' PASSING y AS v) AS je
                FROM (VALUES (2), (3)) b(y)) ss ON x = y
ORDER BY x;
-- pull-up with a DEFAULT expression: JSON_VALUE() is computed below the join.
-- DEFAULT can only reference outer-level columns, which become columns of the
-- parent query once the inner subquery is pulled up.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, jv
                FROM (VALUES (2), (3)) b(y),
                     LATERAL (SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a' RETURNING int
                                                DEFAULT y + 0 ON EMPTY) AS jv) s) ss
     ON x = y
ORDER BY x;
-- so jv is NULL for the unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, jv
                FROM (VALUES (2), (3)) b(y),
                     LATERAL (SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a' RETURNING int
                                                DEFAULT y + 0 ON EMPTY) AS jv) s) ss
     ON x = y
ORDER BY x;
-- OFFSET 0 prevents the pull-up, so jv is NULL for the unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, jv
                FROM (VALUES (2), (3)) b(y),
                     LATERAL (SELECT JSON_VALUE(jsonb '{"a": 1}', '$.a' RETURNING int
                                                DEFAULT y + 0 ON EMPTY) AS jv) s
                OFFSET 0) ss
     ON x = y
ORDER BY x;
-- JSON_QUERY() with DEFAULT ... ON ERROR is computed below the join too
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, y, jq
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, jq
                FROM (VALUES (2), (3)) b(y),
                     LATERAL (SELECT JSON_QUERY(jsonb '{"a": [1]}', '$.a'
                                                DEFAULT to_jsonb(y) ON ERROR) AS jq) s) ss
     ON x = y
ORDER BY x;
-- so jq is NULL for the unmatched x = 1
SELECT x, y, jq
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, jq
                FROM (VALUES (2), (3)) b(y),
                     LATERAL (SELECT JSON_QUERY(jsonb '{"a": [1]}', '$.a'
                                                DEFAULT to_jsonb(y) ON ERROR) AS jq) s) ss
     ON x = y
ORDER BY x;
-- STRICT SQL functions using arguments in PASSING or DEFAULT must not be
-- inlined, as that would make them return non-NULL for NULL input
CREATE FUNCTION queryfuncs_strict_passing(int) RETURNS int
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_VALUE(jsonb '1', '$' PASSING $1 AS v RETURNING int) $$;
CREATE FUNCTION queryfuncs_strict_default(jsonb, int) RETURNS int
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_VALUE($1, '$.a' RETURNING int DEFAULT $2 + 0 ON EMPTY) $$;
-- the plan shows the functions are not inlined
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, queryfuncs_strict_passing(x), queryfuncs_strict_default('{"a": 1}', x)
FROM (VALUES (1), (NULL)) v(x);
-- so they are not called for NULL input, and the results are NULL
SELECT x, queryfuncs_strict_passing(x), queryfuncs_strict_default('{"a": 1}', x)
FROM (VALUES (1), (NULL)) v(x);
-- the same functions in PL/pgSQL are never inlined, and give the same results
CREATE FUNCTION queryfuncs_strict_passing_plpgsql(int) RETURNS int
  LANGUAGE plpgsql IMMUTABLE STRICT
  AS $$ BEGIN
    RETURN JSON_VALUE(jsonb '1', '$' PASSING $1 AS v RETURNING int);
  END $$;
CREATE FUNCTION queryfuncs_strict_default_plpgsql(jsonb, int) RETURNS int
  LANGUAGE plpgsql IMMUTABLE STRICT
  AS $$ BEGIN
    RETURN JSON_VALUE($1, '$.a' RETURNING int DEFAULT $2 + 0 ON EMPTY);
  END $$;
SELECT x, queryfuncs_strict_passing_plpgsql(x),
       queryfuncs_strict_default_plpgsql('{"a": 1}', x)
FROM (VALUES (1), (NULL)) v(x);
DROP FUNCTION queryfuncs_strict_passing_plpgsql,
  queryfuncs_strict_default_plpgsql;
-- STRICT SQL functions using JSON_QUERY() and JSON_EXISTS() (with variable
-- PASSING/default inputs) must not be inlined either.  The one calling
-- to_jsonb() is STABLE, as IMMUTABLE functions with STABLE expressions are
-- never inlined.
CREATE FUNCTION queryfuncs_strict_passing_query(int) RETURNS jsonb
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_QUERY(jsonb '[1]', '$' PASSING $1 AS v) $$;
CREATE FUNCTION queryfuncs_strict_passing_exists(int) RETURNS bool
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_EXISTS(jsonb '1', '$ ? (@ == $v)' PASSING $1 AS v) $$;
CREATE FUNCTION queryfuncs_strict_default_query(jsonb, int) RETURNS jsonb
  LANGUAGE sql STABLE STRICT
  AS $$ SELECT JSON_QUERY($1, '$.a' DEFAULT to_jsonb($2) ON ERROR) $$;
-- the plan shows the functions are not inlined
EXPLAIN (VERBOSE, COSTS OFF)
SELECT x, queryfuncs_strict_passing_query(x),
       queryfuncs_strict_passing_exists(x),
       queryfuncs_strict_default_query('{"a": [1]}', x)
FROM (VALUES (1), (NULL)) v(x);
-- so the results are NULL for NULL input
SELECT x, queryfuncs_strict_passing_query(x),
       queryfuncs_strict_passing_exists(x),
       queryfuncs_strict_default_query('{"a": [1]}', x)
FROM (VALUES (1), (NULL)) v(x);
DROP FUNCTION queryfuncs_strict_passing_query, queryfuncs_strict_passing_exists,
  queryfuncs_strict_default_query;
DROP FUNCTION queryfuncs_strict_passing, queryfuncs_strict_default;

-- A NULL context item is still coerced to a domain RETURNING type, and if the
-- domain rejects NULL, ON ERROR applies.  So with a non-NULL ON ERROR value,
-- the JsonExpr is not strict even in its context item.
CREATE DOMAIN queryfuncs_int_notnull AS int NOT NULL;
CREATE DOMAIN queryfuncs_jsonb_notnull AS jsonb NOT NULL;
-- the ON ERROR values are returned for a NULL context item
SELECT JSON_VALUE(NULL::jsonb, '$' RETURNING queryfuncs_int_notnull DEFAULT 0 ON ERROR),
       JSON_QUERY(NULL::jsonb, '$' RETURNING queryfuncs_jsonb_notnull EMPTY ARRAY ON ERROR);
-- so it must not be treated as strict when pulling up a subquery: jv and jq
-- must be NULL for the unmatched x = 1
SELECT x, y, jv, jq
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y,
                       JSON_VALUE(j, '$' RETURNING queryfuncs_int_notnull
                                  DEFAULT 0 ON ERROR) AS jv,
                       JSON_QUERY(j, '$' RETURNING queryfuncs_jsonb_notnull
                                  EMPTY ARRAY ON ERROR) AS jq
                FROM (VALUES (2, jsonb '2'), (3, jsonb '3')) b(y, j)) ss ON x = y
ORDER BY x;
-- nor when inlining a STRICT SQL function: it must return NULL for NULL input
CREATE FUNCTION queryfuncs_strict_domain(jsonb) RETURNS int
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_VALUE($1, '$' RETURNING queryfuncs_int_notnull
                          DEFAULT 0 ON ERROR) $$;
SELECT x, queryfuncs_strict_domain(x) FROM (VALUES ('1'::jsonb), (NULL)) v(x);
-- the same for JSON_QUERY()
CREATE FUNCTION queryfuncs_strict_domain_query(jsonb) RETURNS jsonb
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_QUERY($1, '$' RETURNING queryfuncs_jsonb_notnull
                          EMPTY ARRAY ON ERROR) $$;
SELECT x, queryfuncs_strict_domain_query(x)
FROM (VALUES ('[1]'::jsonb), (NULL)) v(x);
-- With NULL ON ERROR (the default) or ERROR ON ERROR, the result for a NULL
-- context item is an error, as for a coercion to the domain.
SELECT JSON_VALUE(NULL::jsonb, '$' RETURNING queryfuncs_int_notnull);
-- the same for JSON_QUERY()
SELECT JSON_QUERY(NULL::jsonb, '$' RETURNING queryfuncs_jsonb_notnull);
-- so it must not be treated as strict either: jv must be NULL for the
-- unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_VALUE(j, '$' RETURNING queryfuncs_int_notnull) AS jv
                FROM (VALUES (2, jsonb '2'), (3, jsonb '3')) b(y, j)) ss ON x = y
ORDER BY x;
-- OFFSET 0 prevents the pull-up, so jv is NULL for the unmatched x = 1
SELECT x, y, jv
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_VALUE(j, '$' RETURNING queryfuncs_int_notnull) AS jv
                FROM (VALUES (2, jsonb '2'), (3, jsonb '3')) b(y, j)
                OFFSET 0) ss ON x = y
ORDER BY x;
-- the same for JSON_QUERY(): jq must be NULL for the unmatched x = 1
SELECT x, y, jq
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y, JSON_QUERY(j, '$' RETURNING queryfuncs_jsonb_notnull) AS jq
                FROM (VALUES (2, jsonb '[2]'), (3, jsonb '[3]')) b(y, j)) ss ON x = y
ORDER BY x;
-- ERROR ON ERROR must not expose the coercion to null-extended rows either.
SELECT x, y, jv, jq
FROM (VALUES (1), (2)) a(x)
     LEFT JOIN (SELECT y,
                       JSON_VALUE(j, '$' RETURNING queryfuncs_int_notnull
                                  ERROR ON ERROR) AS jv,
                       JSON_QUERY(j, '$' RETURNING queryfuncs_jsonb_notnull
                                  ERROR ON ERROR) AS jq
                FROM (VALUES (2, jsonb '2'), (3, jsonb '3')) b(y, j)) ss ON x = y
ORDER BY x;
-- A NULL argument must still bypass the STRICT function's body.
CREATE OR REPLACE FUNCTION queryfuncs_strict_domain(jsonb) RETURNS int
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_VALUE($1, '$' RETURNING queryfuncs_int_notnull
                          ERROR ON ERROR) $$;
CREATE OR REPLACE FUNCTION queryfuncs_strict_domain_query(jsonb) RETURNS jsonb
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_QUERY($1, '$' RETURNING queryfuncs_jsonb_notnull
                          ERROR ON ERROR) $$;
SELECT x, queryfuncs_strict_domain(x) AS jv,
       queryfuncs_strict_domain_query(x) AS jq
FROM (VALUES ('1'::jsonb), (NULL)) v(x);
DROP FUNCTION queryfuncs_strict_domain, queryfuncs_strict_domain_query;
DROP DOMAIN queryfuncs_int_notnull, queryfuncs_jsonb_notnull;

-- SQL/JSON domain coercions must be treated as nonstrict even without
-- constraints, since cached plans may lack a dependency on the domain.
CREATE DOMAIN queryfuncs_jsonb_inval AS jsonb;
-- Avoid domain dependencies from coercing ON EMPTY / ON ERROR defaults.
CREATE FUNCTION queryfuncs_strict_domain_inval(jsonb) RETURNS jsonb
  LANGUAGE sql IMMUTABLE STRICT
  AS $$ SELECT JSON_QUERY($1, '$' RETURNING queryfuncs_jsonb_inval
                          ERROR ON EMPTY ERROR ON ERROR) $$;
SET plan_cache_mode = force_generic_plan;
PREPARE queryfuncs_domain_pullup AS
  SELECT x, y, jq
  FROM (VALUES (1), (2)) a(x)
       LEFT JOIN (SELECT y,
                         JSON_QUERY(j, '$' RETURNING queryfuncs_jsonb_inval
                                    ERROR ON EMPTY ERROR ON ERROR) AS jq
                  FROM (VALUES (2, jsonb '[2]'), (3, jsonb '[3]')) b(y, j)) ss
       ON x = y
  ORDER BY x;
PREPARE queryfuncs_domain_inline(jsonb) AS
  SELECT queryfuncs_strict_domain_inval($1) AS jq;

-- Cache both plans while the domain has no constraints.
EXECUTE queryfuncs_domain_pullup;
EXECUTE queryfuncs_domain_inline(NULL);

ALTER DOMAIN queryfuncs_jsonb_inval SET NOT NULL;
-- Adding NOT NULL must not expose the coercion to NULL inputs.
EXECUTE queryfuncs_domain_pullup;
EXECUTE queryfuncs_domain_inline(NULL);

DEALLOCATE queryfuncs_domain_pullup;
DEALLOCATE queryfuncs_domain_inline;
RESET plan_cache_mode;
DROP FUNCTION queryfuncs_strict_domain_inval(jsonb);
DROP DOMAIN queryfuncs_jsonb_inval;
