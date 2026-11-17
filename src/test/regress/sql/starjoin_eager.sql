CREATE TABLE starjoin_eager_h1 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_eager_h2 (id int PRIMARY KEY, v int);
CREATE TABLE starjoin_eager_h3 (id int PRIMARY KEY, v int);

INSERT INTO starjoin_eager_h1 SELECT i, i % 10 FROM generate_series(1,100) i;
INSERT INTO starjoin_eager_h2 SELECT i, i FROM generate_series(1,10000) i;
INSERT INTO starjoin_eager_h3 SELECT i, i FROM generate_series(1,10000) i;

CREATE TABLE starjoin_eager_hf (id int, d1 int NOT NULL, d2 int NOT NULL, d3 int NOT NULL, x int);
INSERT INTO starjoin_eager_hf SELECT i, 1 + i % 100, 1 + (i * 7) % 10000, 1 + (i * 13) % 10000, i % 1000
  FROM generate_series(1,20000) i;

ALTER TABLE starjoin_eager_hf
  ADD FOREIGN KEY (d1) REFERENCES starjoin_eager_h1,
  ADD FOREIGN KEY (d2) REFERENCES starjoin_eager_h2,
  ADD FOREIGN KEY (d3) REFERENCES starjoin_eager_h3;

VACUUM ANALYZE starjoin_eager_h1, starjoin_eager_h2,
               starjoin_eager_h3, starjoin_eager_hf;

SET max_parallel_workers_per_gather = 0;

-- eager agg, GROUP BY first dim (h1), GUC on
SET enable_starjoin_ordering = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT h1.v, sum(hf.x)
FROM starjoin_eager_hf AS hf JOIN starjoin_eager_h1 AS h1 ON hf.d1 = h1.id
                             JOIN starjoin_eager_h2 AS h2 ON hf.d2 = h2.id
                             JOIN starjoin_eager_h3 AS h3 ON hf.d3 = h3.id
GROUP BY h1.v;

-- eager agg, GROUP BY first dim (h1), GUC off
SET enable_starjoin_ordering = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT h1.v, sum(hf.x)
FROM starjoin_eager_hf AS hf JOIN starjoin_eager_h1 AS h1 ON hf.d1 = h1.id
                             JOIN starjoin_eager_h2 AS h2 ON hf.d2 = h2.id
                             JOIN starjoin_eager_h3 AS h3 ON hf.d3 = h3.id
GROUP BY h1.v;

-- same query, h1 written last in FROM, GUC on
SET enable_starjoin_ordering = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT h1.v, sum(hf.x)
FROM starjoin_eager_hf AS hf JOIN starjoin_eager_h2 AS h2 ON hf.d2 = h2.id
                             JOIN starjoin_eager_h3 AS h3 ON hf.d3 = h3.id
                             JOIN starjoin_eager_h1 AS h1 ON hf.d1 = h1.id
GROUP BY h1.v;

-- same query, h1 written last in FROM, GUC off
SET enable_starjoin_ordering = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT h1.v, sum(hf.x)
FROM starjoin_eager_hf AS hf JOIN starjoin_eager_h2 AS h2 ON hf.d2 = h2.id
                             JOIN starjoin_eager_h3 AS h3 ON hf.d3 = h3.id
                             JOIN starjoin_eager_h1 AS h1 ON hf.d1 = h1.id
GROUP BY h1.v;

DROP TABLE starjoin_eager_hf, starjoin_eager_h1, starjoin_eager_h2, starjoin_eager_h3;
