# Goals and Alternatives

- Small, composable components that are easy to understand and extend.
- Use standard data structures like Ibis tables whenever possible.
- Use pure functions and immutable data structures whenever possible.
- Have some built-in algorithms such as a splink-inspired Fellegi-Sunter
  model so you can get going out of the box, but also leave the door open
  for others to plug in their own blocking, comparing, and clustering
  algorithms. If we desgin the interfaces between all these stages carefully,
  then they should all inter-operate and individual components can be swapped
  out for better/custom implementations. Ideally this could position mismo
  as the de-facto experimental platform, similar to how huggingface and pytorch
  are the standard platforms for machine learning.
  platform for researchers. 
- Use duck typing/Protocols to allow users to plug in their own components.
- Wrap the core logic in helper classes to make it easy to get started, but
  have these be shortcuts instead of dead ends.
- Reproducible results using `random_state` similar to sklearn. No peer of Mismo
  seems to do this.
- Ergonomic model persistence.
- Python-first approach. Instead of configuring using JSON, the majority of
  the implementation logic should be in python. This prevents dead ends and makes
  things much more extendable.

## Alternatives to Mismo

A useful way of describing Mismo is to compare it to other record linkage
packages, and what I was trying to do differently.

### [Dedupe](https://www.github.com/dedupeio/dedupe)

Dedupe is pure python, Mismo uses SQL. Thus, Dedupe is able to implement some very
complex algorithms that would be difficult to implement in SQL. However, Mismo
is much much faster than Dedupe, and can handle much larger datasets.

Dedupe is very opinionated, and therefore it is hard to extend. Once you give
it your data, it does everything for you, and it is hard to insert your own
steps in the middle. Mismo is more like a library of building blocks that you
can compose together however you want.

I find that Dedupe's way of saving/loading models was clunky and brittle.
I wanted to make this more ergonomic.

### [Splink](https://github.com/moj-analytical-services/splink)

Splink is a great package, and I learned a lot from it. That is one of the
main sources of inspiration for Mismo.

Like Mismo, Splink uses SQL. However, Splink uses raw SQL, whereas Mismo uses
Ibis. I think the strongly typed, pythonic API of Ibis makes it much easier
for you, as the user of Mismo, to write and maintain code than raw SQL.
It also makes it much easier for me, as the maintainer of Mismo, to write
the algorithms and tests.

Splink only supports a small number of backends at the time of this wriing,
but Ibis, and therefore Mismo, supports many more.

Splink is very opinionated like Dedupe, and suffers from the same problems.

I like how Splink persists model state in a plain json file, but I wish it would
just store the learned weights in there. The parameters that don't change between
runs, such as what comparisons to use, should be implemented in python to give
you the most flexibility. I tried to do this in Mismo.

### [Record Linkage Toolkit](https://github.com/J535D165/recordlinkage)

I really like how RLTK is more of a library than a framework. RLTK is more
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

### [DBLink](https://github.com/cleanzr/dblink)

Bayesian, unsupervised record linkage on structured data using Apache Spark.

Maybe a bit more academic and not production ready. Development has ceased.
Has some interesting algorithms and ideas on how to do things.