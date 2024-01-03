# Fellegi-Sunter Model

The Fellegi-Sunter model is a linear model commonly used for record linkage.
It uses bayesian statistics to assign odds to different levels of agreement
for different dimensions. For example,
"if two names are exactly the same, that makes the odds of a match 7.6 times more likely".
The dimension is "name" and the level of agreement is "exactly the same".
You then combine the multiple dimensions ("name", "location", "price", etc)
together to get a final odds for a record pair.
You can train the weights using labeled data, or from unlabeled data with
an EM (Expectation Maximization) Algorithm.

This is the model used by Splink, and I am trying to copy that over to Mismo.

## Glossary

### Odds

Odds of 4 mean that an event is four times more likely to happen than *not* happen.
I other words, it happens four out of five times.
Odds of 1/4 or .25 is the opposite: it is 4 times more likely to *not* happen than happen.
Odds of 1 means that happening and not happening are equally likely.

Odds can be combined via multiplication, assuming the events are independent.
For example, if you draw two records at random, say the odds of the first name
matching are 1/10_000, and the gender matching is 1/2. Therefore, the combined
odds that the first name matches *AND* the gender matches is
`1/10_000 * 1/2 = 1/20_000`.

In Splink and other contexts, they use the term "Bayes Factor" to mean the same thing
as "odds". I use "odds" because it is shorter, and I think more intuitive to
people who don't know bayesian statistics. It's not that complicated and "odds"
is precise enough.

### Log Odds

The logarithm (base 10) of `Odds`. This is useful because it allows
match weights to be added together rather than being multiplied (as Odds are).
This is useful so we can visualize comparisons between records using a waterfall chart.

For example, with the odds of 100 and 1/10, if we combined these two,
we would get a total odds of `100 * (1/10) = 1/10`.
If we instead combined them using log odds:
The individual log odds would be 2 and -1, and the combined match weight would be
`2 + (-1) = 1`, which corresponds to a `Log Odds` of 1, as expected.

Here is a table to help you get a sense between these two:

| Odds | Log Odds |
|------|----------|
| 0    | -inf     |
| ...  | ...      |
| .01  | -2       |
| .1   | -1       |
| 1    | 0        |
| 10   | 1        |
| 100  | 2        |
| ...  | ...      |
| inf  | inf      |


In Splink and other contexts, they use the term "Match Weight" to mean "Log Odds".
They also use log base 2, but I chose to use log base 10 because I think it
is easier to convert back and forth.

### M probabilities

Amongst record comparisons which are true matches,
what proportion have a match on first name, and what proportion mismatch on first name?

This is a measure of how often mispellings, nicknames
or aliases occur in the first name field.

### U probabilities

Amongst record comparisons which are true non-matches,
what proportion have a match on first name, and what proportion mismatch on first name?

This is a measure of how likely 'collisions' are likely to occur.
For instance, it would be common for two different people to have the same gender,
but less likely for two different people to have the same date of birth.
