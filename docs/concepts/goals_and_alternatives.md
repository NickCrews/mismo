# Goals and Alternatives

- Small, composable components that are easy to understand and extend.
- Use standard data structures like Ibis tables whenever possible.
  Expose these as transparent dataclasses that the user is free to
  reach into directly. The opposite of this would be a heavily
  object-oriented API where all the internal state is intentionally
  hidden from the user: I find this sort of API leaves the user
  with no escape hatch to do the thing they need to do.
  See [this excellent talk by Casey Muratori](https://youtu.be/ZQ5_u8Lgvyk?si=2jOjskkD8Yi5yjyG&t=2911)
  for more on this.
- Use pure functions and immutable data structures whenever possible.
- Have some built-in algorithms such as a splink-inspired Fellegi-Sunter
  model so you can get going out of the box, but also leave the door open
  for others to plug in their own blocking, comparing, and clustering
  algorithms. If we design the interfaces between all these stages carefully,
  then they should all inter-operate and individual components can be swapped
  out for better/custom implementations. Ideally this could position mismo
  as the de-facto experimental platform, similar to how huggingface and pytorch
  are the standard platforms for machine learning.
  platform for researchers. 
- *3rd party IS 1st party*: It should be as easy for 3rd party authors to extend
  mismo as it is to implement those features directly in the core.
  APIs should also be exposed
- Wrap the core logic in helper classes to make it easy to get started, but
  have these be shortcuts instead of dead ends.
- Reproducible results using `random_state` similar to sklearn. No peer of Mismo
  seems to do this.
- Separation between specification and data. eg you define linkage model abstractly,
  without needing the actual tables of data. Then, at a later step, you actually
  apply the model to the data. This makes it so you can re-apply the same model
  to different datasets, eg if you have a linkage job you need to run nightly.
  This also makes it easier to save and load model specifications, 
  share them with others, and adjust them for doing A/B testing.

  This is similar to how in ibis, you define abstract expressions that aren't
  bound to any concrete data, and then only at execution time do you
  need to have the backend and data available.
- Python-first approach. Instead of configuring using JSON, the majority of
  the implementation logic should be in python. This prevents dead ends and makes
  things much more extendable. I found with splink that the JSON representation
  of a model was often inadequate: the data loading and cleaning steps are just
  as important to have a reproducible workflow, but those are only defined in
  python code and are excluded from the JSON.
- Be useful for resolving datasets with disparate schemas: Instead of determining
  "do these records from different datasets refer to the same person?", you should
  also be able to determine "which politician does this campaign contribution go to?".
  In the first case, both records are "people", so they probably have similar schemas.
  In the second case, one record is a person, and the other is a contribution, so
  they likely have different schemas. The leading libraries of `splink` and `dedupe`
  do not account for this.

## What Mismo is NOT good for

- Mismo is targeted for programmers moderately proficient in python.
  It does *not* have GUI or any sort of interface that makes it suitable
  for non-techinical users.
- Having a single JSON file representation of the model. The entire python/notebook
  will need to be stored/shared to reproduce a workflow.

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

### [PyJedAi](https://github.com/AI-team-UoA/pyJedAI)

I don't know about this much. It looks similar to RLTK, except it implements
many more algorithms, and looks more actively maintained. It is maintained
by a group of researchers at University of Athens, so a lot of the algorithms
are state of the art, and documented in related research papers.

I hope to port many of the algorithms over from it, and take inspiration from their API.

It's API is a bit "heavy" though, with a lot of classes, I'm guessing because it is
a port from the [original Java implementation](https://github.com/scify/JedAIToolkit).
I want to make mismo's API more functional and lightweight.

It also suffers from being based around pandas, so it inherintly isn't going to scale
as well as using duckdb and ibis.

### [DBLink](https://github.com/cleanzr/dblink)

Bayesian, unsupervised record linkage on structured data using Apache Spark.

Maybe a bit more academic and not production ready. Development has ceased.
Has some interesting algorithms and ideas on how to do things.