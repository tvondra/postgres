CREATE TABLE starjoin_width_wa (id int PRIMARY KEY, t text);
CREATE TABLE starjoin_width_wb (id int PRIMARY KEY, t text);
CREATE TABLE starjoin_width_wc (id int PRIMARY KEY, v int);

INSERT INTO starjoin_width_wa SELECT i, repeat(md5(i::text), 16) FROM generate_series(1,100) i;
INSERT INTO starjoin_width_wb SELECT i, repeat(md5(i::text), 16) FROM generate_series(1,100) i;
INSERT INTO starjoin_width_wc SELECT i, i FROM generate_series(1,10000) i;

CREATE TABLE starjoin_width_wf (id int, a int NOT NULL REFERENCES starjoin_width_wa, b int NOT NULL REFERENCES starjoin_width_wb, c int NOT NULL REFERENCES starjoin_width_wc);

INSERT INTO starjoin_width_wf SELECT i, 1 + i % 100, 1 + (i * 7) % 100, 1 + (i * 13) % 10000 FROM generate_series(1,10000) i;

VACUUM ANALYZE starjoin_width_wa, starjoin_width_wb, starjoin_width_wc, starjoin_width_wf;

SET max_parallel_workers_per_gather = 0;
SET work_mem = '4MB';
SET enable_nestloop = off; SET enable_mergejoin = off;

-- pure FK joins, GUC on
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT wa.t, wb.t, wc.v FROM starjoin_width_wf AS wf JOIN starjoin_width_wa AS wa ON wf.a = wa.id JOIN starjoin_width_wb AS wb ON wf.b = wb.id JOIN starjoin_width_wc AS wc ON wf.c = wc.id;

--- pure FK joins, GUC off
SET enable_starjoin_ordering = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF) SELECT wa.t, wb.t, wc.v FROM starjoin_width_wf AS wf JOIN starjoin_width_wa AS wa ON wf.a = wa.id JOIN starjoin_width_wb AS wb ON wf.b = wb.id JOIN starjoin_width_wc AS wc ON wf.c = wc.id;

DROP TABLE starjoin_width_wf, starjoin_width_wa, starjoin_width_wb, starjoin_width_wc;
