# Blocking API

Utilities and classes for the blocking phase of record linkage, where
we choose pairs of records to compare.

Without blocking, we would have to compare N*M records, which
becomes intractable for datasets much larger than a few thousand.

## High-Level

::: mismo.block.block
::: mismo.block.BlockingRule
::: mismo.block.ArrayBlocker

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
