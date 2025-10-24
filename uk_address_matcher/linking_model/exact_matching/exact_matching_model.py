from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="annotate_exact_matches",
    description=(
        "Annotate fuzzy addresses with exact hash-join matches on "
        "original_address_concat + postcode"
    ),
    tags=["phase_1", "exact_matching"],
    # Overwrite the input to be the annotated output
    stage_output="fuzzy_addresses",
)
def _annotate_exact_matches() -> list[CTEStep]:
    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    unique_canonical_addresses_sql = """
        SELECT *
        FROM {canonical_addresses} AS canon
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY canon.original_address_concat, canon.postcode
            ORDER BY canon.ukam_address_id
        ) = 1
    """
    annotated_sql = f"""
        SELECT
            fuzzy.unique_id AS unique_id,
            canon.unique_id AS resolved_canonical_id,
            fuzzy.* EXCLUDE (match_reason, unique_id, resolved_canonical_id),
            CASE
                WHEN canon.unique_id IS NOT NULL THEN '{exact_value}'::ENUM {enum_values}
                ELSE fuzzy.match_reason
            END AS match_reason
        FROM {{fuzzy_addresses}} AS fuzzy
        LEFT JOIN {{canonical_exact_matches}} AS canon
          ON fuzzy.original_address_concat = canon.original_address_concat
         AND fuzzy.postcode = canon.postcode
    """
    return [
        CTEStep("canonical_exact_matches", unique_canonical_addresses_sql),
        CTEStep("annotated_exact_matches", annotated_sql),
    ]


@pipeline_stage(
    name="filter_unmatched_matches",
    description="Filter records that haven't been matched yet",
    tags=["phase_1", "exact_matching"],
    stage_output="unmatched_records",
)
def _filter_unmatched_exact_matches() -> list[CTEStep]:
    unmatched_value = MatchReason.UNMATCHED.value
    return f"""
        SELECT
            f.*
        FROM {{input}} AS f
        WHERE f.match_reason = '{unmatched_value}'
    """


# Depends on filter_unmatched_matches, which creates the 'unmatched_records' CTE
@pipeline_stage(
    name="resolve_with_trie",
    description="Build tries for unmatched canonical addresses and resolve remaining fuzzy rows",
    tags=["phase_1", "trie", "exact_matching"],
    # Recommended to be run after `annotate_exact_matches`
    depends_on=["filter_unmatched_matches"],
)
def _resolve_with_trie() -> list[CTEStep]:
    # TODO(ThomasHepworth): When we begin to finalise our API, move the deduplication
    # logic for canonical addresses into a separate reusable step where it is run a single
    # time for all matching strategies.
    filtered_canonical_sql = """
        SELECT
            c.ukam_address_id AS canonical_ukam_address_id,
            c.unique_id AS canonical_unique_id,
            c.postcode,
            LEFT(c.postcode, LENGTH(c.postcode) - 1) AS postcode_group,
            c.address_tokens
        FROM {canonical_addresses} AS c
        WHERE c.unique_id IS NOT NULL
          AND c.postcode IN (
              SELECT DISTINCT postcode
              FROM {unmatched_records}
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.original_address_concat, c.postcode
            ORDER BY c.unique_id
        ) = 1
    """

    tries_sql = """
        SELECT
            postcode_group,
            build_suffix_trie(canonical_ukam_address_id, address_tokens) AS trie
        FROM {canonical_candidates_for_trie}
        GROUP BY postcode_group
    """

    raw_trie_matches_sql = """
        SELECT
            fuzzy.ukam_address_id AS fuzzy_ukam_address_id,
            find_address(fuzzy.address_tokens, tries.trie) AS canonical_ukam_address_id
        FROM {input} AS fuzzy
        JOIN {postcode_group_tries} AS tries
          ON LEFT(fuzzy.postcode, LENGTH(fuzzy.postcode) - 1) = tries.postcode_group
    """

    trie_matches_sql = """
        SELECT
            candidates.fuzzy_ukam_address_id,
            candidates.canonical_ukam_address_id,
            canon.canonical_unique_id
        FROM {raw_trie_matches} AS candidates
        JOIN {canonical_candidates_for_trie} AS canon
          ON candidates.canonical_ukam_address_id = canon.canonical_ukam_address_id
        WHERE candidates.canonical_ukam_address_id IS NOT NULL
    """

    trie_value = MatchReason.TRIE.value
    enum_values = str(MatchReason.enum_values())
    combined_results_sql = f"""
        SELECT
            a.unique_id,
            COALESCE(
                m.canonical_unique_id,
                a.resolved_canonical_id
            ) AS resolved_canonical_id,
            a.* EXCLUDE (unique_id, resolved_canonical_id, match_reason),
            CASE
                WHEN m.canonical_unique_id IS NOT NULL THEN '{trie_value}'::ENUM {enum_values}
                ELSE a.match_reason
            END AS match_reason
        FROM {{fuzzy_addresses}} AS a
        LEFT JOIN {{trie_match_candidates}} AS m
          ON a.ukam_address_id = m.fuzzy_ukam_address_id
    """

    return [
        CTEStep("canonical_candidates_for_trie", filtered_canonical_sql),
        CTEStep("postcode_group_tries", tries_sql),
        CTEStep("raw_trie_matches", raw_trie_matches_sql),
        CTEStep("trie_match_candidates", trie_matches_sql),
        CTEStep("fuzzy_with_resolved_matches", combined_results_sql),
    ]
