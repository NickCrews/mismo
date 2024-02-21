# Blocking API

Utilities and classes for the blocking phase of record linkage, where
we choose pairs of records to compare.

Without blocking, we would have to compare N*M records, which
becomes intractable for datasets much larger than a few thousand.

## High-Level

::: mismo.block.block_one
::: mismo.block.block_many
::: mismo.block.BlockingRule

## Low-level

::: mismo.block.join
::: mismo.block.join_on_array

## Plotting

::: mismo.block.upset_chart

## Analysis

Analyze blocking performance, such as the actual algorithm that
the SQL engine will use. In particular, check for slow O(n*m)
nested loop joins.

::: mismo.block.get_join_algorithm
::: mismo.block.check_join_algorithm
::: mismo.block.JOIN_ALGORITHMS
::: mismo.block.SLOW_JOIN_ALGORITHMS
::: mismo.block.SlowJoinError
::: mismo.block.SlowJoinWarning
