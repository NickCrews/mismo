from __future__ import annotations

import ibis
import pytest

from mismo._common import check_tables_and_links


def _good_left():
    return ibis.memtable({"record_id": [1, 2, 3], "name": ["a", "b", "c"]})


def _good_right():
    return ibis.memtable({"record_id": [10, 20], "name": ["x", "y"]})


def _good_links():
    return ibis.memtable({"record_id_l": [1, 2], "record_id_r": [10, 20]})


def test_happy_path():
    check_tables_and_links(_good_left(), _good_right(), _good_links())


def test_left_missing_record_id():
    left = ibis.memtable({"id": [1, 2]})
    with pytest.raises(ValueError, match="record_id.*not in table"):
        check_tables_and_links(left, _good_right(), _good_links())


def test_right_missing_record_id():
    right = ibis.memtable({"id": [10, 20]})
    with pytest.raises(ValueError, match="record_id.*not in other"):
        check_tables_and_links(_good_left(), right, _good_links())


def test_links_missing_record_id_l():
    links = ibis.memtable({"record_id_r": [10, 20]})
    with pytest.raises(ValueError, match="record_id_l.*not in links"):
        check_tables_and_links(_good_left(), _good_right(), links)


def test_links_missing_record_id_r():
    links = ibis.memtable({"record_id_l": [1, 2]})
    with pytest.raises(ValueError, match="record_id_r.*not in links"):
        check_tables_and_links(_good_left(), _good_right(), links)


def test_left_record_id_incompatible_type():
    left = ibis.memtable({"record_id": ["a", "b"]})
    right = _good_right()
    links = _good_links()
    with pytest.raises(ValueError, match="not comparable"):
        check_tables_and_links(left, right, links)


def test_right_record_id_incompatible_type():
    left = _good_left()
    right = ibis.memtable({"record_id": ["x", "y"]})
    links = _good_links()
    with pytest.raises(ValueError, match="not comparable"):
        check_tables_and_links(left, right, links)
