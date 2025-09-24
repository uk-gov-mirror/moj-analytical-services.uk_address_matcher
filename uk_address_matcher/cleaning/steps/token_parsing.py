from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.core.pipeline_registry import register_step


@register_step(
    name="separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records",
    description="Split address tokens into distinguishing vs common with respect to neighbors",
    group="feature_engineering",
    input_cols=["address_concat"],
    output_cols=["distinguishing_adj_start_tokens", "common_adj_start_tokens"],
)
def _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records() -> (
    list[tuple[str, str]]
):
    tokens_sql = """
    SELECT
        ['FLAT', 'APARTMENT', 'UNIT'] AS __tokens_to_remove,
        list_filter(
            regexp_split_to_array(address_concat, '\\s+'),
            x -> NOT list_contains(__tokens_to_remove, x)
        ) AS __tokens,
        row_number() OVER (ORDER BY reverse(address_concat)) AS row_order,
        *
    FROM {input}
    """
    neighbors_sql = """
    SELECT
        lag(__tokens) OVER (ORDER BY row_order) AS __prev_tokens,
        lead(__tokens) OVER (ORDER BY row_order) AS __next_tokens,
        *
    FROM {tokens}
    """
    suffix_lengths_sql = """
    SELECT
        len(__tokens) AS __token_count,
        CASE WHEN __prev_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__prev_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__prev_tokens), 1, i + 1)
            )
        ELSE 0 END AS prev_common_suffix,
        CASE WHEN __next_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__next_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__next_tokens), 1, i + 1)
            )
        ELSE 0 END AS next_common_suffix,
        *
    FROM {with_neighbors}
    """
    unique_parts_sql = """
    SELECT
        *,
        greatest(prev_common_suffix, next_common_suffix) AS max_common_suffix,
        list_filter(
            __tokens,
            (token, i) -> i < __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS unique_tokens,
        list_filter(
            __tokens,
            (token, i) -> i >= __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS common_tokens
    FROM {with_suffix_lengths}
    """
    final_sql = """
    SELECT
        * EXCLUDE (
            __tokens,
            __prev_tokens,
            __next_tokens,
            __token_count,
            __tokens_to_remove,
            max_common_suffix,
            next_common_suffix,
            prev_common_suffix,
            row_order,
            common_tokens,
            unique_tokens
        ),
        COALESCE(unique_tokens, ARRAY[]) AS distinguishing_adj_start_tokens,
        COALESCE(common_tokens, ARRAY[]) AS common_adj_start_tokens
    FROM {with_unique_parts}
    """
    return [
        ("tokens", tokens_sql),
        ("with_neighbors", neighbors_sql),
        ("with_suffix_lengths", suffix_lengths_sql),
        ("with_unique_parts", unique_parts_sql),
        ("final", final_sql),
    ]


@register_step(
    name="parse_out_flat_position_and_letter",
    description="Extract flat positional information and letter from address",
    group="feature_engineering",
    input_cols=["address_concat"],
    output_cols=["flat_positional", "flat_letter"],
)
def _parse_out_flat_position_and_letter() -> list[tuple[str, str]]:
    """Extract flat positional information and letter from address (multi-CTE)."""
    floor_positions = r"\b(BASEMENT|GROUND FLOOR|FIRST FLOOR|SECOND FLOOR|THIRD FLOOR|TOP FLOOR|GARDEN)\b"
    flat_letter = r"\b\d{0,4}([A-Za-z])\b"
    leading_letter = r"^\s*\d+([A-Za-z])\b"

    flat_number = r"\b(FLAT|UNIT|APARTMENT)\s+(\S*\d\S*)\s+\S*\d\S*\b"

    extract_sql = f"""
    SELECT
        *,
        regexp_extract(address_concat, '{floor_positions}', 1) as floor_pos,
        regexp_extract(address_concat, '{flat_letter}', 1) as flat_letter,
        regexp_extract(address_concat, '{leading_letter}', 1) as leading_letter,
        regexp_extract(address_concat, '{flat_number}', 1) as flat_number
    FROM {{input}}
    """

    final_sql = """
    SELECT
        * EXCLUDE (floor_pos, flat_letter, leading_letter, flat_number),
        NULLIF(floor_pos, '') as flat_positional,
        NULLIF(
            COALESCE(
                NULLIF(flat_letter, ''),
                NULLIF(leading_letter, ''),
                CASE
                    WHEN LENGTH(flat_number) <= 4 THEN flat_number
                    ELSE NULL
                END
            ),
            ''
        ) as flat_letter
    FROM {extract_step}
    """

    return [
        ("extract_step", extract_sql),
        ("final", final_sql),
    ]


@register_step(
    name="parse_out_numbers",
    description="Parse and remove numeric tokens from address, producing address_without_numbers and numeric_tokens",
    group="feature_engineering",
    input_cols=["address_concat", "flat_letter"],
    output_cols=["address_without_numbers", "numeric_tokens"],
)
def _parse_out_numbers() -> str:
    """Extract and process numeric tokens from address_concat."""
    regex_pattern = (
        r"\b"  # Word boundary
        # Prioritize matching number ranges first
        r"(\d{1,5}-\d{1,5}|[A-Za-z]?\d{1,5}[A-Za-z]?)"
        r"\b"  # Word boundary
    )
    sql = f"""
    SELECT
        * EXCLUDE (address_concat),
        regexp_replace(address_concat, '{regex_pattern}', '', 'g') AS address_without_numbers,
        CASE
            WHEN flat_letter IS NOT NULL AND flat_letter ~ '^\\d+$' THEN
            regexp_extract_all(address_concat, '{regex_pattern}')[2:]
            ELSE
                regexp_extract_all(address_concat, '{regex_pattern}')
        END AS numeric_tokens
    FROM {{input}}
    """
    return sql


@register_step(
    name="clean_address_string_second_pass",
    description="Second pass cleaning on address_without_numbers (spacing, trim)",
    group="cleaning",
    input_cols=["address_without_numbers"],
    output_cols=["address_without_numbers"],
)
def _clean_address_string_second_pass() -> str:
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    select
        * exclude (address_without_numbers),
        {fn_call} as address_without_numbers
    from {{input}}
    """
    return sql

    # legacy duplicates removed: split_numeric_tokens_to_cols, tokenise_address_without_numbers


GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
    CASE
        WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
        WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
        WHEN token in ('BASEMENT') THEN ['LEVEL']
        ELSE [TOKEN]
    END

"""


@register_step(
    name="generalised_token_aliases",
    description="Apply token aliasing to distinguishing_adj_start_tokens",
    group="feature_engineering",
    input_cols=["distinguishing_adj_start_tokens"],
    output_cols=["distinguishing_adj_token_aliases"],
)
def _generalised_token_aliases() -> str:
    sql = f"""
    SELECT
        *,
        flatten(
            list_transform(distinguishing_adj_start_tokens, token ->
               {GENERALISED_TOKEN_ALIASES_CASE_STATEMENT}
            )
        ) AS distinguishing_adj_token_aliases
    FROM {{input}}
    """
    return sql


def _get_token_frequeny_table():
    """Moved to term_frequencies module; kept for backward-compat imports."""
    raise NotImplementedError(
        "Use uk_address_matcher.cleaning.steps.term_frequencies.get_token_frequeny_table instead"
    )
