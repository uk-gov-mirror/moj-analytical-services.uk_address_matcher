from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    replace_fwd_slash_with_dash,
    separate_letter_num,
    standarise_num_letter,
    trim,
)
from uk_address_matcher.core.pipeline_registry import register_step


# -------- Enhanced registered steps (new system, private only) --------
@register_step(
    name="trim_whitespace_address_and_postcode",
    description="Trim whitespace from address and postcode",
    group="cleaning",
    input_cols=["address_concat", "postcode"],
    output_cols=["address_concat", "postcode"],
)
def _trim_whitespace_address_and_postcode() -> str:
    return r"""
    SELECT
        * EXCLUDE (address_concat, postcode),
        TRIM(address_concat) AS address_concat,
        TRIM(postcode)       AS postcode
    FROM {input}
    """


@register_step(
    name="upper_case_address_and_postcode",
    description="Upper-case normalisation for address and postcode",
    group="cleaning",
    input_cols=["address_concat", "postcode"],
    output_cols=["address_concat", "postcode"],
)
def _upper_case_address_and_postcode() -> str:
    return r"""
    SELECT
        * EXCLUDE (address_concat, postcode),
        UPPER(address_concat) AS address_concat,
        UPPER(postcode)       AS postcode
    FROM {input}
    """


@register_step(
    name="canonicalise_postcode",
    description="Ensure UK postcode has a single space between outward and inward codes",
    group="cleaning",
    input_cols=["postcode"],
    output_cols=["postcode"],
    depends_on=[
        "trim_whitespace_address_and_postcode",
        "upper_case_address_and_postcode",
    ],
)
def _canonicalise_postcode() -> str:
    uk_postcode_regex: str = r"^([A-Z]{1,2}\d[A-Z\d]?|GIR)\s*(\d[A-Z]{2})$"
    return f"""
    SELECT
        * EXCLUDE (postcode),
        regexp_replace(
            postcode,
            '{uk_postcode_regex}',
            -- Reformat with single space
            '\\1 \\2'
        ) AS postcode
    FROM {{input}}
    """


@register_step(
    name="clean_address_string_first_pass",
    description="First pass cleaning on address_concat (punctuation, spacing, standardisation)",
    group="cleaning",
    input_cols=["address_concat"],
    output_cols=["address_concat"],
)
def _clean_address_string_first_pass() -> str:
    fn_call = construct_nested_call(
        "address_concat",
        [
            remove_commas_periods,
            remove_apostrophes,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            separate_letter_num,
            standarise_num_letter,
            move_flat_to_front,
            trim,
        ],
    )
    return f"""
    SELECT
        * EXCLUDE (address_concat),
        {fn_call} AS address_concat
    FROM {{input}}
    """


@register_step(
    name="remove_duplicate_end_tokens",
    description="Remove duplicated trailing tokens from address_concat",
    group="cleaning",
    input_cols=["address_concat"],
    output_cols=["address_concat"],
    depends_on=["clean_address_string_first_pass"],
)
def _remove_duplicate_end_tokens() -> str:
    return r"""
    WITH tokenised AS (
        SELECT *, string_split(address_concat, ' ') AS cleaned_tokenised
        FROM {input}
    )
    SELECT
        * EXCLUDE (cleaned_tokenised, address_concat),
        CASE
            WHEN array_length(cleaned_tokenised) >= 2
                 AND cleaned_tokenised[-1] = cleaned_tokenised[-2]
            THEN array_to_string(cleaned_tokenised[:-2], ' ')
            WHEN array_length(cleaned_tokenised) >= 4
                 AND cleaned_tokenised[-4] = cleaned_tokenised[-2]
                 AND cleaned_tokenised[-3] = cleaned_tokenised[-1]
            THEN array_to_string(cleaned_tokenised[:-3], ' ')
            ELSE address_concat
        END AS address_concat
    FROM tokenised
    """


@register_step(
    name="derive_original_address_concat",
    description="Copy address_concat to original_address_concat",
    group="cleaning",
    input_cols=["address_concat"],
    output_cols=["original_address_concat"],
)
def _derive_original_address_concat() -> str:
    return r"""
    SELECT
        *,
        address_concat AS original_address_concat
    FROM {input}
    """
