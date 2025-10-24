import pytest

from uk_address_matcher.linking_model.exact_matching.exact_matching_model import (
    _annotate_exact_matches,
    _filter_unmatched_exact_matches,
    _resolve_with_trie,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline


@pytest.fixture
def enum_values() -> str:
    return str(MatchReason.enum_values())


@pytest.fixture
def trie_input_bindings(duck_con, enum_values) -> list[InputBinding]:
    duck_con.execute(
        f"""
        CREATE OR REPLACE TABLE fuzzy_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '4 Sample Street',
                    'CC3 3CC',
                    NULL::BIGINT,
                    ARRAY['4', 'sample', 'street'],
                    'unmatched'::ENUM {enum_values},
                    1::BIGINT
                ),
                (
                    10,
                    '4 Sample Street',
                    'CC3 3CC',
                    NULL::BIGINT,
                    ARRAY['4', 'sample', 'street'],
                    'unmatched'::ENUM {enum_values},
                    2::BIGINT
                ),
                (
                    2,
                    '5 Demo Rd',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'rd'],
                    'unmatched'::ENUM {enum_values},
                    3::BIGINT
                ),
                (
                    2,
                    '5 Demo Rd',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'rd'],
                    'unmatched'::ENUM {enum_values},
                    4::BIGINT
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'road'],
                    'unmatched'::ENUM {enum_values},
                    5::BIGINT
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'road'],
                    'unmatched'::ENUM {enum_values},
                    6::BIGINT
                ),
                (
                    2,
                    '4 Sample St',
                    'CC3 3CC',
                    NULL::BIGINT,
                    ARRAY['4', 'sample', 'st'],
                    'unmatched'::ENUM {enum_values},
                    7::BIGINT
                ),
                (
                    3,
                    '999 Mystery Lane',
                    'EE5 5EE',
                    NULL::BIGINT,
                    ARRAY['999', 'mystery', 'lane'],
                    'unmatched'::ENUM {enum_values},
                    8::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            resolved_canonical_id,
            address_tokens,
            match_reason,
            ukam_address_id
        )
        """
    )

    duck_con.execute(
        """
        CREATE OR REPLACE TABLE canonical_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1000,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    1
                ),
                (
                    2000,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    2
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens,
            ukam_address_id
        )
        """
    )

    return [
        InputBinding("fuzzy_addresses", duck_con.table("fuzzy_addresses")),
        InputBinding("canonical_addresses", duck_con.table("canonical_addresses")),
    ]


def test_unmatched_records_retain_original_unique_id(duck_con, trie_input_bindings):
    pipeline = create_sql_pipeline(
        duck_con,
        trie_input_bindings,
        [_annotate_exact_matches, _filter_unmatched_exact_matches, _resolve_with_trie],
    )

    results = pipeline.run()

    columns = set(results.columns)
    assert "unique_id" in columns
    assert "resolved_canonical_id" in columns
    assert "fuzzy_unique_id" not in columns
    assert "exact_match_canonical_id_bigint" not in columns
    assert "trie_match_unique_id_bigint" not in columns
    assert "resolved_canonical_unique_id_bigint" not in columns

    rows = (
        results.project(
            "unique_id, resolved_canonical_id, match_reason, ukam_address_id"
        )
        .order("ukam_address_id")
        .fetchall()
    )
    assert rows == [
        (1, 1000, MatchReason.EXACT.value, 1),
        (10, 1000, MatchReason.EXACT.value, 2),
        (2, 2000, MatchReason.EXACT.value, 3),
        (2, 2000, MatchReason.EXACT.value, 4),
        (2, 2000, MatchReason.TRIE.value, 5),
        (2, 2000, MatchReason.TRIE.value, 6),
        (2, 1000, MatchReason.TRIE.value, 7),
        (3, None, MatchReason.UNMATCHED.value, 8),
    ]


# When a non-unique unique_id field exists in our fuzzy addresses,
# the trie stage will inflate our row count (due to the output and required
# joins). This test checks confirms that this issue does not occur.
# We've resolved this issue by implementing a ukam_address_id surrogate key
# to guarantee uniqueness of the input records.
@pytest.mark.parametrize(
    "stages",
    [
        [_annotate_exact_matches, _filter_unmatched_exact_matches, _resolve_with_trie],
        [_filter_unmatched_exact_matches, _resolve_with_trie],
    ],
)
def test_trie_stage_does_not_inflate_row_count(duck_con, trie_input_bindings, stages):
    pipeline = create_sql_pipeline(
        duck_con,
        trie_input_bindings,
        stages,
    )

    results = pipeline.run()

    input_row_count = duck_con.table("fuzzy_addresses").count("*").fetchone()[0]
    total_rows = results.count("*").fetchone()[0]
    output_ids = results.order("ukam_address_id").project("ukam_address_id").fetchall()
    input_ids = (
        duck_con.table("fuzzy_addresses")
        .order("ukam_address_id")
        .project("ukam_address_id")
        .fetchall()
    )

    assert total_rows == input_row_count, (
        "Trie pipeline should not increase row count; "
        f"expected {input_row_count}, got {total_rows}"
    )
    assert output_ids == input_ids, "Pipeline must preserve ukam_address_id coverage"
