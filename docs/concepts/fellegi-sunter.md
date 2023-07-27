# Glossary of the Fellegi-Sunter Model

## Bayes Factor

Bayes factors can be explained in words. For example, a Bayes Factor of 20 for a
given column means that an overall match is now 20 times more probable.
A Bayes Factor of 1/10 for a given column means an overall
match is 10 times less probable.

In this sense, a Bayes Factor is are similar to the concept of odds.
Odds of 4 mean that an event happens four out of five times,
or in some sense it is four times more likely for the event to happen than not happen.

However, Bayes Factors differ from odds in that they are only meaningful
in the context of a prior. A Bayes Factor is an adjustment - it tells us
something is more or less likely. But we need a starting value -
otherwise there's nothing to apply the adjustment to.

## Match Weight

The logarithm of Bayes Factors. This is useful because it allows
match weights to be added together rather than being multiplied (as Bayes Factors are).
This is useful so we can visualize comparisons between records using a waterfall chart.

For example, with the bayes factors 8 and 1/2, if we combined these two,
we would get a Bayes Factor of `8 * (1/2) = 4`. The corresponding match weights
(using a log base of 2) would be 3 and -1, and the combined match weight would be
`3 + (-1) = 2`, which corresponds to a Bayes Factor of 4, as expected.

## Prior

A prior is a starting value for a Bayes Factor or match weight. Generally,
we start with the probability that two random records from our two datasets
are a match. From there, we can adjust the probability up or down based on
the values of the columns.

## M probabilities

Amongst record comparisons which are true matches,
what proportion have a match on first name, and what proportion mismatch on first name?

This is a measure of how often mispellings, nicknames
or aliases occur in the first name field.

## U probabilities

Amongst record comparisons which are true non-matches,
what proportion have a match on first name, and what proportion mismatch on first name?

This is a measure of how likely 'collisions' are likely to occur.
For instance, it would be common for two different people to have the same gender,
but less likely for two different people to have the same date of birth.
