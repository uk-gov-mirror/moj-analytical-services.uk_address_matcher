from __future__ import annotations

from uk_address_matcher.core.pipeline_registry import register_step


@register_step(
    name="split_numeric_tokens_to_cols",
    description="Extract up to three numeric tokens from numeric_tokens array",
    group="feature_engineering",
    input_cols=["numeric_tokens"],
    output_cols=["numeric_token_1", "numeric_token_2", "numeric_token_3"],
)
def _split_numeric_tokens_to_cols() -> str:
    return """
    SELECT
        * EXCLUDE (numeric_tokens),
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[1] as numeric_token_1,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[2] as numeric_token_2,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[3] as numeric_token_3
    FROM {input}
    """


@register_step(
    name="tokenise_address_without_numbers",
    description="Tokenise address_without_numbers into an array",
    group="feature_engineering",
    input_cols=["address_without_numbers"],
    output_cols=["address_without_numbers_tokenised"],
)
def _tokenise_address_without_numbers() -> str:
    return """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
