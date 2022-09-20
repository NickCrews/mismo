import pytest

from mismo.block._dedupe import IdPairsBlocking

ids = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3)]
id_pairs = [((id1, {"a": "b"}), (id2, {"c": "d"})) for id1, id2 in ids]


@pytest.mark.parametrize(
    "blocking",
    [
        IdPairsBlocking(ids),
        IdPairsBlocking.from_pairs(id_pairs),
    ],
    ids=["ids", "id_pairs"],
)
def test_DedupeBlocking__to_arrow(blocking: IdPairsBlocking) -> None:
    assert blocking.n_pairs is None

    expected = {
        "left": [0, 0, 0, 1, 1],
        "right": [1, 2, 3, 2, 3],
    }
    assert blocking.to_arrow().to_pydict() == expected


@pytest.mark.parametrize(
    "blocking",
    [
        IdPairsBlocking(ids),
        IdPairsBlocking.from_pairs(id_pairs),
    ],
    ids=["ids", "id_pairs"],
)
def test_DedupeBlocking__iter_arrow(blocking: IdPairsBlocking) -> None:
    assert blocking.n_pairs is None

    chunks = list(blocking.iter_arrow(chunk_size=2))
    assert len(chunks) == 3
    assert chunks[0].to_pydict() == {"left": [0, 0], "right": [1, 2]}
    assert chunks[1].to_pydict() == {"left": [0, 1], "right": [3, 2]}
    assert chunks[2].to_pydict() == {"left": [1], "right": [3]}
