# Blocking API

Utilities and classes for the blocking phase of record linkage, where
we choose pairs of records to compare.

## High-Level

::: mismo.block.block
::: mismo.block.join
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
::: mismo.block.JOIN_ALGORTITHMS
::: mismo.block.SLOW_JOIN_ALGORTITHMS
::: mismo.block.SlowJoinError
::: mismo.block.SlowJoinWarning
