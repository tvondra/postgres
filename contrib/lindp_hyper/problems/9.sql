-- not a problem, looks good

-- Build a deterministic star schema large enough to trigger GEQO at the
-- default threshold.  Only some dimension tables have selective predicates,
-- which makes the join order important.
DO $$
BEGIN
	EXECUTE 'DROP TABLE IF EXISTS geqo_hub';
	EXECUTE 'CREATE TABLE geqo_hub(id int primary key)';
	EXECUTE 'INSERT INTO geqo_hub SELECT generate_series(1, 1000)';

	EXECUTE 'DROP TABLE IF EXISTS geqo_fact';
	EXECUTE 'CREATE TABLE geqo_fact(hub_id int not null, k int not null)';
	EXECUTE 'INSERT INTO geqo_fact
			 SELECT ((g * 37) % 1000) + 1,
					CASE WHEN g % 50 = 0 THEN 1 ELSE 2 + (g % 50) END
			 FROM generate_series(1, 8000) g';

	EXECUTE 'CREATE INDEX ON geqo_fact(k, hub_id)';
	EXECUTE 'CREATE INDEX ON geqo_fact(hub_id)';

	FOR i IN 1..11 LOOP
		EXECUTE format('DROP TABLE IF EXISTS geqo_dim%s', i);
		EXECUTE format('CREATE TABLE geqo_dim%s(hub_id int not null, k int not null)', i);

		-- geqo_dim1..geqo_dim5 make k = 1 selective; geqo_dim6..geqo_dim11
		-- make k = 1 intentionally non-selective.
		IF i <= 5 THEN
			EXECUTE format('INSERT INTO geqo_dim%s
							SELECT ((g * %s) %% 1000) + 1,
								   CASE WHEN g %% 100 = 0 THEN 1 ELSE 2 + (g %% 100) END
							FROM generate_series(1, 2000) g', i, 17 + i * 4);
		ELSE
			EXECUTE format('INSERT INTO geqo_dim%s
							SELECT ((g * %s) %% 1000) + 1,
								   CASE WHEN g %% 10 <> 0 THEN 1 ELSE 2 + (g %% 100) END
							FROM generate_series(1, 6000) g', i, 17 + i * 4);
		END IF;

		EXECUTE format('CREATE INDEX ON geqo_dim%s(k, hub_id)', i);
		EXECUTE format('CREATE INDEX ON geqo_dim%s(hub_id)', i);
	END LOOP;
END;
$$;

ANALYZE;

set enable_parallel_append = off;
set max_parallel_workers_per_gather = 0;
set join_collapse_limit = 20;
set from_collapse_limit = 20;
set geqo = on;
set geqo_threshold = 2;
set geqo_effort = 5;

\timing on

EXPLAIN
SELECT count(*) FROM geqo_hub h
JOIN geqo_dim11 ON geqo_dim11.hub_id = h.id
JOIN geqo_dim5 ON geqo_dim5.hub_id = h.id
JOIN geqo_dim2 ON geqo_dim2.hub_id = h.id
JOIN geqo_fact ON geqo_fact.hub_id = h.id
JOIN geqo_dim1 ON geqo_dim1.hub_id = h.id
JOIN geqo_dim9 ON geqo_dim9.hub_id = h.id
JOIN geqo_dim6 ON geqo_dim6.hub_id = h.id
JOIN geqo_dim7 ON geqo_dim7.hub_id = h.id
JOIN geqo_dim10 ON geqo_dim10.hub_id = h.id
JOIN geqo_dim4 ON geqo_dim4.hub_id = h.id
JOIN geqo_dim3 ON geqo_dim3.hub_id = h.id
JOIN geqo_dim8 ON geqo_dim8.hub_id = h.id
WHERE geqo_fact.k = 1
  AND geqo_dim1.k = 1
  AND geqo_dim2.k = 1
  AND geqo_dim3.k = 1
  AND geqo_dim6.k = 1
  AND geqo_dim8.k = 1
  AND geqo_dim9.k = 1
  AND geqo_dim10.k = 1
  AND geqo_dim11.k = 1;

LOAD 'lindp_hyper';
SET lindp_hyper.min_relations = 2;
SET lindp_hyper.max_relations = 64;

EXPLAIN
SELECT count(*) FROM geqo_hub h
JOIN geqo_dim11 ON geqo_dim11.hub_id = h.id
JOIN geqo_dim5 ON geqo_dim5.hub_id = h.id
JOIN geqo_dim2 ON geqo_dim2.hub_id = h.id
JOIN geqo_fact ON geqo_fact.hub_id = h.id
JOIN geqo_dim1 ON geqo_dim1.hub_id = h.id
JOIN geqo_dim9 ON geqo_dim9.hub_id = h.id
JOIN geqo_dim6 ON geqo_dim6.hub_id = h.id
JOIN geqo_dim7 ON geqo_dim7.hub_id = h.id
JOIN geqo_dim10 ON geqo_dim10.hub_id = h.id
JOIN geqo_dim4 ON geqo_dim4.hub_id = h.id
JOIN geqo_dim3 ON geqo_dim3.hub_id = h.id
JOIN geqo_dim8 ON geqo_dim8.hub_id = h.id
WHERE geqo_fact.k = 1
  AND geqo_dim1.k = 1
  AND geqo_dim2.k = 1
  AND geqo_dim3.k = 1
  AND geqo_dim6.k = 1
  AND geqo_dim8.k = 1
  AND geqo_dim9.k = 1
  AND geqo_dim10.k = 1
  AND geqo_dim11.k = 1;
