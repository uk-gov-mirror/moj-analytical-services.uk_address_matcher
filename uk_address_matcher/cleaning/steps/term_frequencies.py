from __future__ import annotations


import importlib.resources as pkg_resources
from uk_address_matcher.core.pipeline_registry import register_step


@register_step(
    name="add_term_frequencies_to_address_tokens",
    description="Compute relative frequencies for tokens and attach as token_rel_freq_arr",
    group="feature_engineering",
    input_cols=["unique_id", "address_without_numbers_tokenised"],
    output_cols=["token_rel_freq_arr"],
)
def _add_term_frequencies_to_address_tokens() -> list[tuple[str, str]]:
    base_sql = """
    SELECT * FROM {input}
    """
    rel_tok_freq_cte_sql = """
    SELECT
        token,
        count(*) / sum(count(*)) OVER () AS rel_freq
    FROM (
        SELECT
            unnest(address_without_numbers_tokenised) AS token
        FROM {base}
    )
    GROUP BY token
    """
    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """
    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE({rel_tok_freq_cte}.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN {rel_tok_freq_cte}
        ON {addresses_exploded}.token = {rel_tok_freq_cte}.token
    """
    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """
    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """
    return [
        ("base", base_sql),
        ("rel_tok_freq_cte", rel_tok_freq_cte_sql),
        ("addresses_exploded", addresses_exploded_sql),
        ("address_groups", address_groups_sql),
        ("token_freq_lookup", token_freq_lookup_sql),
        ("final", final_sql),
    ]


@register_step(
    name="add_term_frequencies_to_address_tokens_using_registered_df",
    description="Attach precomputed token rel frequencies from rel_tok_freq relation",
    group="feature_engineering",
    input_cols=["unique_id", "address_without_numbers_tokenised"],
    output_cols=["token_rel_freq_arr"],
)
def _add_term_frequencies_to_address_tokens_using_registered_df() -> (
    list[tuple[str, str]]
):
    base_sql = """
    SELECT * FROM {input}
    """
    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """
    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE(rel_tok_freq.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN rel_tok_freq
        ON {addresses_exploded}.token = rel_tok_freq.token
    """
    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """
    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """
    return [
        ("base", base_sql),
        ("addresses_exploded", addresses_exploded_sql),
        ("address_groups", address_groups_sql),
        ("token_freq_lookup", token_freq_lookup_sql),
        ("final", final_sql),
    ]


@register_step(
    name="move_common_end_tokens_to_field",
    description="Move common trailing tokens to separate field and filter from rel_freq array",
    group="feature_engineering",
    input_cols=["token_rel_freq_arr"],
    output_cols=["token_rel_freq_arr", "common_end_tokens"],
)
def _move_common_end_tokens_to_field() -> list[tuple[str, str]]:
    base_sql = """
    SELECT * FROM {input}
    """
    with pkg_resources.path(
        "uk_address_matcher.data", "common_end_tokens.csv"
    ) as csv_path:
        common_end_tokens_sql = f"""
            select array_agg(token) as end_tokens_to_remove
            from read_csv_auto('{csv_path}')
            where token_count > 3000
            """
    joined_sql = """
    SELECT *
    FROM {base}
    CROSS JOIN {common_end_tokens}
    """
    end_tokens_included_sql = """
    SELECT
        * EXCLUDE (end_tokens_to_remove),
        list_filter(
            token_rel_freq_arr[-3:],
            x -> list_contains(end_tokens_to_remove, x.tok)
        ) AS common_end_tokens
    FROM {joined}
    """
    remove_end_tokens_expr = """
    list_filter(
        token_rel_freq_arr,
        (x, i) -> NOT (
            i > len(token_rel_freq_arr) - 2
            AND list_contains(list_transform(common_end_tokens, x -> x.tok), x.tok)
        )
    )
    """
    final_sql = f"""
    SELECT
        * EXCLUDE (token_rel_freq_arr),
        {remove_end_tokens_expr} AS token_rel_freq_arr
    FROM {{end_tokens_included}}
    """
    return [
        ("base", base_sql),
        ("common_end_tokens", common_end_tokens_sql),
        ("joined", joined_sql),
        ("end_tokens_included", end_tokens_included_sql),
        ("final", final_sql),
    ]


@register_step(
    name="first_unusual_token",
    description="Attach first token with rel_freq < 0.001 if present",
    group="feature_engineering",
    input_cols=["token_rel_freq_arr"],
    output_cols=["first_unusual_token"],
)
def _first_unusual_token() -> str:
    first_token_expr = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )
    return f"""
    SELECT
        {first_token_expr} AS first_unusual_token,
        *
    FROM {{input}}
    """


@register_step(
    name="use_first_unusual_token_if_no_numeric_token",
    description="Fallback: use first_unusual_token if numeric_token_1 missing",
    group="feature_engineering",
    input_cols=["numeric_token_1", "token_rel_freq_arr", "first_unusual_token"],
    output_cols=["numeric_token_1", "token_rel_freq_arr"],
)
def _use_first_unusual_token_if_no_numeric_token() -> str:
    return """
    SELECT
        * EXCLUDE (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        CASE
            WHEN numeric_token_1 IS NULL THEN first_unusual_token.tok
            ELSE numeric_token_1
        END AS numeric_token_1,
        CASE
            WHEN numeric_token_1 IS NULL THEN
                list_filter(
                    token_rel_freq_arr,
                    x -> coalesce(x.tok != first_unusual_token.tok, TRUE)
                )
            ELSE token_rel_freq_arr
        END AS token_rel_freq_arr
    FROM {input}
    """


@register_step(
    name="separate_unusual_tokens",
    description="Split token_rel_freq_arr into unusual/very/extremely_unusual arrays",
    group="feature_engineering",
    input_cols=["token_rel_freq_arr"],
    output_cols=[
        "unusual_tokens_arr",
        "very_unusual_tokens_arr",
        "extremely_unusual_tokens_arr",
    ],
)
def _separate_unusual_tokens() -> str:
    return """
    SELECT
        *,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-4 AND x.rel_freq >= 5e-5
            ),
            x -> x.tok
        ) AS unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 5e-5 AND x.rel_freq >= 1e-7
            ),
            x -> x.tok
        ) AS very_unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-7
            ),
            x -> x.tok
        ) AS extremely_unusual_tokens_arr
    FROM {input}
    """


@register_step(
    name="final_column_order",
    description="Reorder columns and aggregate histograms for final output",
    group="feature_engineering",
    input_cols=[
        "unique_id",
        "numeric_token_1",
        "numeric_token_2",
        "numeric_token_3",
        "token_rel_freq_arr",
        "common_end_tokens",
        "postcode",
    ],
    output_cols=[
        "unique_id",
        "numeric_token_1",
        "numeric_token_2",
        "numeric_token_3",
        "token_rel_freq_arr_hist",
        "common_end_tokens_hist",
        "postcode",
    ],
)
def _final_column_order() -> str:
    return """
    SELECT
        unique_id,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        list_aggregate(token_rel_freq_arr, 'histogram') AS token_rel_freq_arr_hist,
        list_aggregate(common_end_tokens, 'histogram') AS common_end_tokens_hist,
        postcode,
        * EXCLUDE (
            unique_id,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            token_rel_freq_arr,
            common_end_tokens,
            postcode
        )
    FROM {input}
    """


@register_step(
    name="get_token_frequeny_table",
    description="Compute token frequency table from concatenated numeric and non-numeric tokens",
    group="analytics",
    input_cols=[
        "unique_id",
        "numeric_token_1",
        "numeric_token_2",
        "numeric_token_3",
        "address_without_numbers_tokenised",
    ],
    output_cols=["token", "rel_freq"],
)
def _get_token_frequeny_table() -> list[tuple[str, str]]:
    sql = """
    SELECT
        token_counts.token,
        token_counts.rel_freq
    FROM (
        SELECT
            token,
            COUNT(*) AS count,
            COUNT(*) / (SELECT COUNT(*) FROM {unnested}) AS rel_freq
        FROM {unnested}
        GROUP BY token
    ) AS token_counts
    ORDER BY count DESC
    """
    concatenated_sql = """
    SELECT
        unique_id,
        list_concat(
            array_filter(
                [numeric_token_1, numeric_token_2, numeric_token_3],
                x -> x IS NOT NULL
            ),
            address_without_numbers_tokenised
        ) AS all_tokens
    FROM {input}
    """
    unnested_sql = """
    SELECT unnest(all_tokens) AS token
    FROM {concatenated_tokens}
    """
    return [
        ("concatenated_tokens", concatenated_sql),
        ("unnested", unnested_sql),
        ("final", sql),
    ]
