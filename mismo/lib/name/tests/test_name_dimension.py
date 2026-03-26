from __future__ import annotations

import mismo
from mismo.lib.name import NameDimension, NameMatchLevel
from mismo.tests.util import assert_tables_equal


def test_name_dimension(name_table, table_factory):
    dim = NameDimension("name")
    name_table = dim.prepare_for_fast_linking(name_table)
    name_table = dim.prepare_for_blocking(name_table)
    expected = table_factory(
        [
            ("a_anderson_phd", "a_anderson_phd", int(NameMatchLevel.EXACT)),
            ("a_anderson_phd", "alice_anderson", int(NameMatchLevel.INITIALS)),
            ("bob_baker", "robert_b_baker_jr", int(NameMatchLevel.NICKNAMES)),
            (
                "charles_carter",
                "mr_charles_carter",
                int(NameMatchLevel.GIVEN_SURNAME),
            ),
            ("a_anderson_phd", "<null>", int(NameMatchLevel.NULL)),
            ("alice_anderson", "charles_carter", int(NameMatchLevel.ELSE)),
        ],
        schema={
            "record_id_l": "string",
            "record_id_r": "string",
            "name_compared": "int8",
        },
    )
    links = mismo.FullLinker(task="link")(name_table, name_table).links
    compared = dim.compare(links)
    compared = compared.semi_join(expected, ["record_id_l", "record_id_r"])
    compared = compared.cache()
    compared = compared.select(expected.columns)
    assert_tables_equal(expected, compared)
