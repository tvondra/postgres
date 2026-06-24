join_search_lindp
=================

`join_search_lindp` is a prototype PostgreSQL module that replaces the
genetic query optimizer (GEQO) -- and, optionally, the regular
dynamic-programming join search -- with a join-order search based on
**search-space linearization**.

It implements the ideas from two papers:

* **LinDP++** -- Radke & Neumann,
  <https://db.in.tum.de/~radke/papers/lindp++.pdf>
* **Adaptive LinDP** -- Birler et al.,
  <https://db.in.tum.de/~birler/papers/adaptivelindp.pdf>

The module is self-contained: it installs the public `join_search_hook` and
relies only on the same planner entry points used by `standard_join_search()`
and GEQO, so no core planner files are modified.


How it works
------------

When the hook fires for a join of `levels_needed` relations, the module runs
the following phases:

1. **Graph extraction.**  The entries of `initial_rels` become the nodes of a
   join graph.  Edges (and approximate join selectivities) are discovered with
   `have_relevant_joinclause()` and the relations' `joininfo` clauses.  If the
   graph is not connected, it is decomposed into its connected components;
   LinDP is run on each component independently and the resulting per-component
   join relations are combined with a final `standard_join_search()` (which
   only has to enumerate the Cartesian products between the components).  The
   module does **not** fall back to `standard_join_search()` for the whole
   problem just because the graph is disconnected.

2. **Linearization (IKKBZ).**  A spanning tree of the join graph is built
   (minimum-selectivity Kruskal), and the polynomial **IKKBZ** algorithm
   computes a high-quality linear order of the relations under an ASI cost
   model.  IKKBZ is run once per candidate root; the cheapest order is used.
   With `seeds > 1`, several of the best orders are kept.

3. **Linearized dynamic programming.**  A DP runs *restricted to the linear
   order*: only contiguous subsequences -- optionally with a bounded number of
   "holes" (the window) -- are considered as subproblems.  Each subproblem is
   built with `make_join_rel()`, so PostgreSQL's real cost model and full
   bushy/parallel/partitionwise path construction are used within the window.
   Each materialized joinrel is finalized exactly as `standard_join_search()`
   does (`generate_partitionwise_join_paths()`,
   `generate_useful_gather_paths()`, `set_cheapest()`, grouped-rel paths).

4. **Adaptive widening.**  When `adaptive` is on, the window is enlarged while
   the estimated number of DP subproblems stays within the `effort` budget,
   recovering plan quality lost to a hard linearization (adaptive LinDP).

If several seeds are evaluated, each is run in an isolated memory context
(restoring `join_rel_list`/`join_rel_hash`, in the style of `geqo_eval()`),
the cheapest is selected, and only the winner is re-run for real.  If a
connected component cannot be built within the LinDP budget, the module falls
back to `standard_join_search()` for that component only, so a valid plan is
always returned.


Configuration
-------------

All GUCs are namespaced under `join_search_lindp.`:

| GUC             | Type | Default | Meaning                                              |
|-----------------|------|---------|------------------------------------------------------|
| `enabled`       | bool | `on`    | Master switch; when off, chains to any previous hook or the standard search. |
| `min_relations` | int  | `8`     | Engage only at or above this many relations.         |
| `max_relations` | int  | `64`    | Above this many relations, fall back.                |
| `window_size`   | int  | `1`     | Base number of "holes" allowed in a DP subproblem (`0` = pure contiguous-interval LinDP). |
| `adaptive`      | bool | `on`    | Adaptively widen `window_size` within the effort budget. |
| `effort`        | int  | `20000` | Budget, in DP subproblems, for adaptive widening.    |
| `seeds`         | int  | `1`     | Number of alternative linearizations to evaluate.    |


Usage
-----

    LOAD 'join_search_lindp';
    SET join_search_lindp.min_relations = 6;
    -- ... run large multi-way join queries ...

Or preload it for all sessions via `shared_preload_libraries` /
`session_preload_libraries`.


Tests
-----

`make check` runs `sql/join_search_lindp.sql`, which exercises chain, star,
clique/cyclic, outer-join, disconnected, and adaptive/multi-seed cases.  Its
core oracle compares each query's result set with the linearized search
enabled versus disabled, asserting they are identical -- i.e. the linearized
search must return the same answer the standard join search would.
