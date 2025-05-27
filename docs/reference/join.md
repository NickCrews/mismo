## Join Utilities

::: mismo.join
::: mismo.left
::: mismo.right

## Analyze: Join Algorithm
Analyze the actual algorithm that the SQL engine will use when
pairing up records during the join.
In particular, check for slow `O(n*m)` nested loop joins.

See [https://duckdb.org/2022/05/27/iejoin.html]() for a very good
explanation of how SQL engines (or at least duckdb) chooses
a join algorithm.

::: mismo.joins.get_join_algorithm
::: mismo.joins.check_join_algorithm
::: mismo.joins.JOIN_ALGORITHMS
::: mismo.joins.SLOW_JOIN_ALGORITHMS
::: mismo.joins.SlowJoinError
::: mismo.joins.SlowJoinWarning