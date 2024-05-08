from __future__ import annotations

from io import BytesIO
import os
import sys
from typing import Hashable
from zipfile import ZipFile

import pandas as pd
from requests import get


def main():
    """
    Adapt the
    ["Affiliations" dataset](https://dbs.uni-leipzig.de/index.php/research/projects/benchmark-datasets-for-entity-resolution)
    from the Database Group at Leipzig University to mismo's standard dataset format.
    The original dataset is provided under the Creative Commons license (CC BY 4.0).

    This depends on the source dataset archive at
    https://github.com/OlivierBinette/ul-benchmark-datasets-for-entity-resolution-archive/raw/1.0.0/Entity%20Resolution/affiliationstrings.zip

    This writes an "affiliations.csv" file with "record_id", "label_true", and
    "affiliation" columns. When invoked as a script, the csv file is saved to
    the same directory as the script's source code.

    !!! Note

        The ground truth labels are not very reliable.
    """

    DATA_SOURCE_URL = "https://github.com/OlivierBinette/ul-benchmark-datasets-for-entity-resolution-archive/raw/1.0.0/Entity%20Resolution/affiliationstrings.zip"
    OUTPUT_FILEPATH = os.path.join(sys.path[0], "affiliations.csv")

    affiliations, matching_pairs = get_dataset(DATA_SOURCE_URL)
    affiliations.insert(
        0, "label_true", make_components_labels(affiliations, matching_pairs)
    )
    affiliations.to_csv(OUTPUT_FILEPATH, index=True)


def get_dataset(source_url):
    response = get(source_url)
    response.raise_for_status()

    with ZipFile(BytesIO(response.content)) as file:
        with file.open("affiliationstrings_ids.csv") as affiliationstrings_ids:
            ids = pd.read_csv(affiliationstrings_ids, dtype="string")
        with file.open("affiliationstrings_mapping.csv") as affiliationstrings_mapping:
            matching_pairs = pd.read_csv(
                affiliationstrings_mapping, header=None, dtype="string"
            )

    affiliations = ids.rename(columns={"id1": "record_id", "affil1": "affiliation"})
    affiliations.set_index("record_id", inplace=True)

    return affiliations, matching_pairs


def make_components_labels(affiliations, matching_pairs):
    graph = dict()
    for index in affiliations.index:
        graph[index] = []
    for _, row in matching_pairs.iterrows():
        graph[row[0]].append(row[1])
        # Mapping already contains edges going both way.
        # No need for `graph[row[1]].append(row[0])`

    return pd.Series(connected_components_labels(graph))


def connected_components_labels(
    graph: dict[Hashable, list],
) -> dict[Hashable, Hashable]:
    labeling = dict()

    for node_id in graph.keys():
        if node_id not in labeling:
            label_component(node_id, graph, labeling)

    return labeling


def label_component(
    root_node_id: Hashable,
    graph: dict[Hashable, list],
    labeling: dict[Hashable, Hashable],
) -> None:
    to_visit = {root_node_id}
    while True:
        if len(to_visit) == 0:
            break

        next_visits = set()
        for node_id in to_visit:
            labeling[node_id] = root_node_id
            next_visits.update(x for x in graph[node_id] if x not in labeling)
        to_visit = next_visits


if __name__ == "__main__":
    main()
