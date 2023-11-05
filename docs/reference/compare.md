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

These utility functions help you create [ComparisonLevel](#mismo.compare.ComparisonLevel)

::: mismo.compare.exact_level
::: mismo.compare.jaccard
::: mismo.compare.distance_km


## Felleni-Sunter Model

The Felleni-Sunter model is a popular model for record linkage.
It uses bayesian statistics and basic machine learning to assign
weights to different ComparisonLevels, and then combine them
together to get a final score for a record pair.

::: mismo.compare.fs.Weights
::: mismo.compare.fs.ComparisonWeights
::: mismo.compare.fs.LevelWeights
::: mismo.compare.fs.train_comparison
::: mismo.compare.fs.plot_weights
