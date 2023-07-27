# Alternatives to Mismo

A useful way of describing Mismo is to compare it to other record linkage
packages, and what I was trying to do differently.

## [Dedupe](https://www.github.com/dedupeio/dedupe)

Dedupe is pure python, Mismo uses SQL. Thus, Dedupe is able to implement some very
complex algorithms that would be difficult to implement in SQL. However, Mismo
is much much faster than Dedupe, and can handle much larger datasets.


## [Splink](https://github.com/moj-analytical-services/splink)

## [Record Linkage Toolkit](https://github.com/J535D165/recordlinkage)

I really like how RLTK is more of a library than a framework.
Dedupe and Splink are "all in one" solutions that have a strong opinion on how
to do things. This made it hard to inject custom behavior inside. RLTK is more
like sklearn, in the sense that it is a collection of tools that you can
combine how you want. I wanted to emulate this. But in addition, I wanted to add
a framework layer on top of this, so you could have an easy to get going
and opinionated solution like Dedupe and Splink.

RLTK uses pandas and numpy, so it suffers scaling limitations, because you
can't work with data that is going to be larger than memory.

I liked how the data structures RLTK expects/returns from its APIs were all
standard stuff like pandas dataframes and numpy arrays. I tried to copy this
as much as possible, and the result is that Mismo uses vanilla Ibis tables
whenever possible.

I didn't like how RLTK was so object oriented. It seems like you have to create
an object to perform most tasks. These objects are also mutable, which makes
it hard to keep track of their state. Whenever possible, I tried to use
pure functions and immutable data structuress.
