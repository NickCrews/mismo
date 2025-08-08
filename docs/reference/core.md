# Core API

Types, functions, etc that are core to mismo and are used throughout the rest
of the framework

## Core Types

::: mismo.Linkage
::: mismo.LinkedTable
::: mismo.LinksTable
::: mismo.CountsTable
::: mismo.UnionTable

## Linkers
Utilities and classes for the blocking phase of record linkage, where
we choose pairs of records to compare.

Without blocking, we would have to compare N*M records, which
becomes intractable for datasets much larger than a few thousand.

::: mismo.Linker
::: mismo.FullLinker
::: mismo.EmptyLinker
::: mismo.JoinLinker
::: mismo.KeyLinker
::: mismo.KeyLinker.key_counts_left
::: mismo.KeyLinker.key_counts_right
::: mismo.OrLinker
::: mismo.linkage.sample_all_links

## Comparing tables

::: mismo.Updates
::: mismo.Diff
::: mismo.DiffStats
::: mismo.LinkCountsTable
