# Clustering API

After we have compared pairs of records, we need to somehow resolve these links
into groups of records that are all the same entity. This is done with various
graph algorithms, which are implemented in this module.

## Algorithms

::: mismo.cluster.connected_components
::: mismo.cluster.degree

## Evaluation

Utilities for assessing the quality of a linkage result.

::: mismo.cluster.mutual_info_score
::: mismo.cluster.adjusted_mutual_info_score
::: mismo.cluster.normalized_mutual_info_score
::: mismo.cluster.rand_score
::: mismo.cluster.adjusted_rand_score
::: mismo.cluster.fowlkes_mallows_score
::: mismo.cluster.homogeneity_score
::: mismo.cluster.completeness_score
::: mismo.cluster.v_measure_score
::: mismo.cluster.homogeneity_completeness_v_measure

## Plot

::: mismo.cluster.degree_dashboard
::: mismo.cluster.cluster_dashboard
::: mismo.cluster.clusters_dashboard