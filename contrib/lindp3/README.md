lindp — LinDP++ join-order search
=================================

`lindp` is a loadable module that replaces PostgreSQL's join-order search for
queries with many relations.  It installs a `join_search_hook` that uses
*search-space linearization* (the LinDP++ algorithm) instead of GEQO's genetic
algorithm.

Background
----------

For a query joining *n* relations, exhaustive dynamic programming (the in-core
`standard_join_search`) considers all `2^n` relation subsets and quickly
becomes too expensive, which is why PostgreSQL falls back to GEQO once the
number of relations reaches `geqo_threshold`.

LinDP++ takes a different, deterministic approach:

1. **Linearization (IKKBZ).**  The join graph is turned into a single linear
   order of the relations using the IKKBZ algorithm.  IKKBZ computes, for every
   possible root of the (rooted) query tree, the cost-optimal left-deep order
   under the ASI (adjacent-sequence-interchange) cost model, by ranking
   relations and contracting precedence-violating sub-sequences.

2. **Linearized dynamic programming.**  Dynamic programming is then run, but
   restricted to *contiguous intervals* of that linear order.  Every bushy
   split of an interval is considered, so the result is the optimal plan whose
   relation sets are contiguous in the linearization.  This costs `O(n^3)`
   instead of `O(2^n)`.

3. **Adaptive linearization.**  Several candidate linearizations (seeded from
   different IKKBZ roots) are tried and the cheapest resulting plan is kept.

References:

* Radke, Neumann: *LinDP++: Generalizing Linearized DP to Crossproducts and
  Non-Inner Joins*, https://db.in.tum.de/~radke/papers/lindp++.pdf
* Birler, Radke, Neumann: adaptive LinDP,
  https://db.in.tum.de/~birler/papers/adaptivelindp.pdf

Supported queries
-----------------

All join shapes are supported, because every candidate join is built with the
in-core `make_join_rel()`, which enforces all join-order restrictions:

* **Cross products** — join graphs with disconnected components.  Each
  component is solved independently and the components are then combined with
  cross-product joins.
* **Non-inner joins** — outer/semi/anti joins and their order restrictions.
* **Complex join clauses** — clauses that reference more than two relations
  (join hyperedges).

If a chosen linear order happens to admit no legal decomposition of the full
relation set (for instance, an outer-join order restriction incompatible with
the order), the module transparently falls back to `standard_join_search`, so
a valid plan is always produced.

Usage
-----

Load the module and (optionally) tune it:

    LOAD 'lindp';                 -- or add to shared_preload_libraries
    -- now LinDP++ is used for join problems with >= lindp.threshold relations

Because it installs a `join_search_hook`, the module overrides GEQO: when
LinDP++ declines a problem (e.g. too few relations) it uses the exhaustive
search rather than GEQO.

Configuration parameters
------------------------

* `lindp.enabled` (boolean, default `on`)
  When off, the hook simply delegates to the in-core join search.

* `lindp.threshold` (integer, default `12`)
  Minimum number of relations for which LinDP++ is used.  Smaller join
  problems use exact dynamic programming.

* `lindp.max_relations` (integer, default `0`)
  Upper bound on the number of relations LinDP++ will handle; larger problems
  fall back to the in-core search.  `0` means no limit.

* `lindp.adaptive` (boolean, default `on`)
  Try several linearizations and keep the cheapest plan.

* `lindp.seeds` (integer, default `5`)
  Number of linearizations tried per connected component when
  `lindp.adaptive` is on.
