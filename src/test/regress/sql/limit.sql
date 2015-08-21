CREATE OR REPLACE FUNCTION ct_limits(numcols integer, colstoreflag boolean)
RETURNS boolean
LANGUAGE plpgsql
AS
$$
DECLARE
 sql TEXT;
 i	INTEGER;
BEGIN
 	sql := 'CREATE TABLE colstore_limit (';
	<< col >>
	FOR i IN 1..numcols LOOP
		IF i > 1 THEN
			sql = sql || ',';
		END IF;
		sql := sql || 'col' || i::text || ' smallint';
		IF colstoreflag THEN
		  sql := sql || ' COLUMN STORE foo' || i::text || ' USING store_am_one';
		END IF;
	END LOOP col;
	sql := sql || ')';

	BEGIN
		EXECUTE sql;
	EXCEPTION
		WHEN OTHERS THEN
			RETURN false;
	END;
	RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION check_limits(colstoreflag boolean)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
 i INTEGER := 2;
 flag TEXT := 'false';
BEGIN
 IF colstoreflag THEN
 	flag := 'true';
 END IF;
 << main >>
 LOOP
	BEGIN
		EXECUTE 'DROP TABLE IF EXISTS colstore_limit';
		EXECUTE 'SELECT ct_limits(' || i::text || ',' || flag || ')';
		EXECUTE 'INSERT INTO colstore_limit DEFAULT VALUES';
		RAISE NOTICE '%',i;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Failed at %', i;
			RETURN;
	END;
	i := i + 2;
 END LOOP main;
END;
$$;
SELECT check_limits(false);
SELECT check_limits(true);
