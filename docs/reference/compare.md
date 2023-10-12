# Comparing API

Once records are blocked together into pairs, we actually can do pairwise
comparisons on them.

## Comparison Objects

A `Comparisons` is an unordered collection of `Comparison` objects.
Each `Comparison` represents a dimension to compare against, such as
*name*, *location*, *price*, *date*, etc.
Each one contains many `ComparisonLevel`s, each of which is a level of aggreement,
such as *exact*, *misspelling*, *within_1_km*, etc.

::: mismo.compare.Comparisons
::: mismo.compare.Comparison
::: mismo.compare.ComparisonLevel

## Comparison Functions

These utility functions help you create `ComparisonLevel` objects.

::: mismo.compare.exact_level
::: mismo.compare.intersection_n
::: mismo.compare.jaccard
::: mismo.compare.distance_km
::: mismo.compare.fs


## Felleni-Sunter Model

::: mismo.compare.fs.FellegiSunterComparer
::: mismo.compare.fs.Weights
::: mismo.compare.fs.ComparisonWeights
::: mismo.compare.fs.LevelWeights
::: mismo.compare.fs.train_comparison
