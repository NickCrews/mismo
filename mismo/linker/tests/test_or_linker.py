from __future__ import annotations

import warnings

import ibis
import pytest

import mismo
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def people_left(table_factory):
    """A simple test dataset representing people."""
    return table_factory(
        {
            "record_id": [0, 1, 2, 3, 4],
            "name": [
                "Alice Smith",
                "Bob Jones",
                "Charlie Brown",
                "Diana Prince",
                "Eve Adams",
            ],
            "age": [25, 30, 35, 28, 22],
            "city": ["New York", "Boston", "Chicago", "Seattle", "Denver"],
            "email": [
                "alice@email.com",
                "bob@email.com",
                "charlie@email.com",
                "diana@email.com",
                "eve@email.com",
            ],
        }
    )


@pytest.fixture
def people_right(table_factory):
    """Another simple test dataset representing people with some matches."""
    return table_factory(
        {
            "record_id": [10, 11, 12, 13, 14],
            "name": [
                "Alice Smith",
                "Robert Jones",
                "Charles Brown",
                "Wonder Woman",
                "Eve A",
            ],
            "age": [25, 30, 36, 28, 22],
            "city": ["New York", "Boston", "Chicago", "Seattle", "Denver"],
            "email": [
                "alice@email.com",
                "robert@email.com",
                "cb@email.com",
                "ww@email.com",
                "eve@email.com",
            ],
        }
    )


def test_or_linker_empty_conditions(table_factory, people_left, people_right):
    """Test OrLinker with no conditions returns empty linkage."""
    linker = mismo.OrLinker([])
    linkage = linker(people_left, people_right)
    expected = table_factory([], schema=linkage.links.schema())
    assert_tables_equal(expected, linkage.links)


def test_or_linker_single_condition(table_factory, people_left, people_right):
    """Test OrLinker with a single condition."""
    conditions = ["name"]
    linker = mismo.OrLinker(conditions)
    linkage = linker(people_left, people_right)

    actual = linkage.links.select("record_id_l", "record_id_r").order_by(
        "record_id_l", "record_id_r"
    )
    expected = table_factory(
        [
            (0, 10),  # Alice Smith
        ],
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual, order_by=["record_id_l", "record_id_r"])


@pytest.mark.parametrize(
    "condition_getter",
    [
        pytest.param(lambda: ("name", "email"), id="string_conditions"),
        pytest.param(lambda: (ibis._.name, ibis._.email), id="deferred_conditions"),
        pytest.param(
            lambda: (mismo.KeyLinker("name"), mismo.KeyLinker("email")),
            id="key_linker_conditions",
        ),
        pytest.param(
            lambda: (
                lambda left, right: left.name == right.name,
                lambda left, right: left.email == right.email,
            ),
            id="lambda_conditions",
        ),
        pytest.param(lambda: ("name", mismo.KeyLinker("email")), id="mixed_conditions"),
        # including False shouldn't affect the linkage
        pytest.param(
            lambda: ("name", mismo.KeyLinker("email"), False), id="with_false"
        ),
        pytest.param(
            lambda: (("name", "name"), mismo.KeyLinker("email")), id="tuple_conditions"
        ),
    ],
)
def test_or_linker_multiple_conditions(
    table_factory, people_left, people_right, condition_getter
):
    """Test OrLinker with multiple conditions (OR logic)."""
    conditions = condition_getter()
    linker = mismo.OrLinker(conditions)
    linkage = linker(people_left, people_right)

    actual = linkage.links.select("record_id_l", "record_id_r").order_by(
        "record_id_l", "record_id_r"
    )
    expected = table_factory(
        [
            (0, 10),  # Alice Smith - matches both name and email
            # even though Alice Smith matches multiple conditions, it doesn't appear
            # twice, because we removed overlapping conditions.
            (4, 14),  # Eve Adams / Eve A - matches email
        ],
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual, order_by=["record_id_l", "record_id_r"])


def test_or_linker_with_named_conditions(table_factory, people_left, people_right):
    """Test OrLinker with named conditions (dict input)."""
    conditions = {
        "name_match": "name",
        "email_match": "email",
    }
    linker = mismo.OrLinker(conditions)
    linkage = linker(people_left, people_right)

    # Check that we can access the join_conditions by name
    assert "name_match" in linker.join_conditions
    assert "email_match" in linker.join_conditions

    # Should match on name: Alice Smith (1,10), Charles Brown (3,12)
    # Should match on email: Alice Smith (1,10), Eve Adams (5,14)
    actual = linkage.links.select("record_id_l", "record_id_r").order_by(
        "record_id_l", "record_id_r"
    )
    expected = table_factory(
        [
            (0, 10),  # Alice Smith - matches both name and email
            (4, 14),  # Eve Adams / Eve A - matches email
        ],
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual, order_by=["record_id_l", "record_id_r"])


def test_or_linker_deduplication(table_factory):
    """Test OrLinker on deduplication (same table)."""
    people = table_factory(
        {
            "record_id": [1, 2, 3, 4],
            "name": ["Alice Smith", "Alice Smith", "Bob Jones", "Charlie Brown"],
            "age": [25, 25, 30, 35],
            "email": [
                "alice@email.com",
                "alice2@email.com",
                "bob@email.com",
                "charlie@email.com",
            ],
        }
    )

    conditions = ["name", "age"]
    linker = mismo.OrLinker(conditions)
    linkage = linker(people, people)

    # For deduplication, we expect record_id_l < record_id_r
    # Should match on name: Alice Smith records (1,2)
    # Should match on age: Alice Smith records (1,2) - already covered by name
    actual = linkage.links.select("record_id_l", "record_id_r").order_by(
        "record_id_l", "record_id_r"
    )
    expected = table_factory(
        [
            (1, 2),  # Alice Smith records - match on both name and age
        ],
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual, order_by=["record_id_l", "record_id_r"])


def test_or_linker_on_slow_parameter(people_left, people_right):
    """Test OrLinker with on_slow parameter."""
    # This should not raise an error even with potentially slow conditions
    conditions = [
        lambda left, right: left.name.levenshtein(right.name) < 2,  # Potentially slow
    ]

    # Test with ignore
    linker = mismo.OrLinker(conditions, on_slow="ignore")
    linkage = linker(people_left, people_right)
    assert linkage is not None

    # Test with warn (should warn but not error)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        linker = mismo.OrLinker(conditions, on_slow="warn")
        linkage = linker(people_left, people_right)
        assert isinstance(linkage, mismo.Linkage)

    with pytest.raises(mismo.exceptions.SlowJoinError):
        # Test with error (should raise SlowJoinError)
        linker = mismo.OrLinker(conditions, on_slow="error")
        linker(people_left, people_right)


def test_or_linker_properties():
    """Test OrLinker properties and methods."""
    conditions = {
        "name_match": "name",
        "email_match": "email",
    }
    linker = mismo.OrLinker(conditions)

    # Test join_conditions property
    assert len(linker.join_conditions) == 2
    assert "name_match" in linker.join_conditions
    assert "email_match" in linker.join_conditions

    # Test that each condition has __join_condition__ method
    for condition in linker.join_conditions.values():
        assert hasattr(condition, "__join_condition__")
