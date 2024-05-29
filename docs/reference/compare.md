# Comparing API

Once records are blocked together into pairs, we actually can do pairwise
comparisons on them.

## High-level

::: mismo.compare.compare

## Level-Based Comparisons

Bin record pairs into discrete levels, based on levels of agreement.

Each [LevelComparer](#mismo.compare.LevelComparer) represents a dimension,
such as *name*, *location*, *price*, *date*, etc.
Each one contains many [AgreementLevel](#mismo.compare.AgreementLevel)s,
each of which is a level of aggreement,
such as *exact*, *misspelling*, *within_1_km*, etc.

::: mismo.compare.LevelComparer
::: mismo.compare.AgreementLevel
::: mismo.compare.MatchLevels

## Plotting

::: mismo.compare.compared_dashboard

