# Comparing API

Once records are blocked together into pairs, we actually can do pairwise
comparisons on them.

## Comparison Objects

A [Comparisons](#mismo.compare.Comparisons) is an unordered dict-like collection of
[Comparison](#mismo.compare.Comparison)s.
Each [Comparison](#mismo.compare.Comparison) represents a dimension to compare against, such as
*name*, *location*, *price*, *date*, etc.
Each one contains many [ComparisonLevel](#mismo.compare.Comparison)s, each of which is a level of aggreement,
such as *exact*, *misspelling*, *within_1_km*, etc.

::: mismo.compare.Comparisons
::: mismo.compare.Comparison
::: mismo.compare.ComparisonLevel

## Comparison Functions

These utility functions help you create [ComparisonLevels](#mismo.compare.ComparisonLevel)

::: mismo.compare.jaccard
::: mismo.compare.distance_km

