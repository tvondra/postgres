lindp
=====

`lindp_hyper` is a small loadable module that installs a `join_search_hook`
implementing the **LinDP++** join-ordering algorithm described in

> T. Neumann, B. Radke. *LinDP++: Generalizing Linearized DP to Crossproducts
> and Non-Inner Joins*, BTW 2018. https://db.in.tum.de/~radke/papers/lindp++.pdf

LinDP++ is a *linearized* dynamic programming algorithm. Instead of the
classic Selinger-style DP, whose search space grows exponentially with the
number of relations, it first computes a good linear order of the relations
and then runs dynamic programming restricted to that order. This makes it
practical for join problems that are too large for the standard search but
where GEQO's randomized search is undesirable.

The module is a drop-in replacement: load it (via `LOAD`,
`session_preload_libraries`, or `shared_preload_libraries`) and the planner
will use it for join searches that meet the configured size thresholds.  When
LinDP++ cannot produce a legal plan it transparently falls back to the
standard join search / GEQO, so results are always correct.


Other papers
------------

To better understand how LinDP, it may be helpful to read some of the earlier
(and later) papers describing the algorithm / approach, in particular:

> T. Neumann, B. Radke. *Adaptive Optimization of Very Large Join Queries*,
> SIGMOD’18. https://db.in.tum.de/~radke/papers/hugejoins.pdf

> A Birler, M. Stoian, T. Neumann. *Optimizing Linearized Join Enumeration
> by Adapting to the Query Structure*, BTW 2025.
> https://db.in.tum.de/~birler/papers/adaptivelindp.pdf


How it works
------------

The implementation mirrors the structure of the paper:

1. **Hypergraph construction.**

   The relations to be joined become the vertices of a hypergraph. Join
   predicates become simple edges (annotated with an estimated selectivity),
   outer/semi/anti joins become *hyperedges* that record which vertices live
   inside the right-hand side of the join, and disconnected components are
   bridged with synthetic *cross* edges.

2. **Generalized IKKBZ linearization (Algorithm 2 in the paper).**

   The hypergraph is turned into a single linear order of relations. Outer-join
   hyperedges are handled by a recursive *precedence-graph decomposition*: the
   vertex set is split at a hyperedge into its two sides, each side is
   linearized independently, and the two linear orders are concatenated so
   that the sides remain contiguous (and therefore joinable). Each connected,
   simple sub-problem is linearized with the classic rank-based IKKBZ tree
   algorithm (build a precedence tree, then normalize and merge sub-chains by
   their ASI rank).

3. **Linearized dynamic programming.**  A dynamic program runs over the linear
   order, only ever joining two *contiguous* sub-sequences.  This bounds the
   work to O(n^3) `make_join_rel()` calls while still allowing bushy plans.
   Splits without a join predicate are built as cross products, which -- with
   the cross edges from step 1 -- realizes the paper's heuristic of enriching
   the search space with cross products.

4. **Adaptive seeding.**  The linearization is only a heuristic, so several
   seed relations are tried as IKKBZ roots, each linearization is costed, and
   the cheapest plan is kept.


Building the actual join relations and paths is delegated to the core
`make_join_rel()`, which enforces every join-legality rule, so the plans
produced are always valid.


Configuration parameters
------------------------

`lindp_hyper.enabled` (`boolean`)
    Turns the hook on or off.  Default `on`.

`lindp_hyper.min_relations` (`integer`)
    Smallest join problem (number of relations) for which LinDP++ engages.
    Smaller problems use the standard join search. Default `2`.

`lindp_hyper.max_relations` (`integer`)
    Largest join problem for which LinDP++ engages; `0` (the default) means no
    limit.  Above the limit the standard join search (or GEQO) is used.

`lindp_hyper.seeds` (`integer`)
    Number of seed relations tried as IKKBZ roots.  More seeds explore more
    linearizations at the cost of planning time.  `0` tries every relation.
    Default `5`.

`lindp_hyper.cross_products` (`boolean`)
    When `on` (the default), the search space is enriched with cross products
    and disconnected join problems are handled directly.  When `off`, the
    disconnected components are planned using linearization, but then fall
    back to the standard join search.


Questions
---------

* The LinDP++ paper claims that thanks to handling non-inner joins (which the
  earlier LinDP variants could not reliably), it can linearize any query:

>  ... First, we generalize the underlying linearization strategy to handle
> non-inner joins, which allows us to linearize the search space of arbitrary
> queries. ... This results in a very generic join ordering framework that
> can handle arbitrary queries and produces excellent results over the whole
> range of query sizes.

  Is that actually true. Is it guaranteed there are no queries where this would
  fail to produce a valid linearization? It probably depends on whether we
  generate hyperedges for all relevant restrictions.

* Are there any join-legality rules enforced by `make_join_rel()` that are
  not represented by a hyper edge?

* We probably want to leave small problems to the exhaustive DP algorithm,
  wich means the `min_relations` default is a bit too low. We should probably
  set it to `9` or something like that?
