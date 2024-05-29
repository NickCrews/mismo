# Comparing API

Once records are blocked together into pairs, we actually can do pairwise
comparisons on them.

All of the APIs revolve around the [](#mismo.compare.PComparer) protocol.
This is simply a function which takes a table of record pairs,
(eg with columns suffixed with `_l` and `_r`), and returns a modified
version of this table. For example, it could add a column with match scores,
add rows that were missed during the initial blocking, or remove rows
that we no longer want to consider as matched.

::: mismo.compare.PComparer

## Level-Based Comparers

Bin record pairs into discrete levels, based on levels of agreement.

Each [LevelComparer](#mismo.compare.LevelComparer) represents a dimension,
such as *name*, *location*, *price*, *date*, etc.
Each one contains many [MatchLevel](#mismo.compare.MatchLevel)s,
each of which is a level of aggreement,
such as *exact*, *misspelling*, *within_1_km*, etc.

::: mismo.compare.MatchLevel
::: mismo.compare.LevelComparer

## Plotting

::: mismo.compare.compared_dashboard

