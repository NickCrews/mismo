# Blocking API
Utilities and classes for the blocking phase of record linkage, where
we choose pairs of records to compare.

Without blocking, we would have to compare N*M records, which
becomes intractable for datasets much larger than a few thousand.

## Blockers
::: mismo.block.PBlocker
::: mismo.block.CrossBlocker
::: mismo.block.EmptyBlocker
::: mismo.block.ConditionBlocker

## Utils
::: mismo.block.n_naive_comparisons
::: mismo.block.sample_all_pairs
::: mismo.block.join

## Key-Based Blockers
Generate pairs where records share a single key, eg "first_name"

::: mismo.block.KeyBlocker

## Set-Based Blockers
Generate pairs where two sets have a high degree of member overlap.

::: mismo.block.MinhashLshBlocker
::: mismo.block.minhash_lsh_keys
::: mismo.block.plot_lsh_curves

## Ensemble Blockers
Blockers that use other Blockers

::: mismo.block.UnionBlocker
::: mismo.block.upset_chart

## Analyze: Join Algorithm
Analyze the actual algorithm that the SQL engine will use when
pairing up records during the join.
In particular, check for slow `O(n*m)` nested loop joins.

See [https://duckdb.org/2022/05/27/iejoin.html]() for a very good
explanation of how SQL engines (or at least duckdb) chooses
a join algorithm.

::: mismo.block.get_join_algorithm
::: mismo.block.check_join_algorithm
::: mismo.block.JOIN_ALGORITHMS
::: mismo.block.SLOW_JOIN_ALGORITHMS
::: mismo.block.SlowJoinError
::: mismo.block.SlowJoinWarning
