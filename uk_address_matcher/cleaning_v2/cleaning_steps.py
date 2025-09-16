from uk_address_matcher.cleaning_v2.pipeline import (
    CTEStep,
    Stage,
)


def trim_whitespace_address_and_postcode() -> Stage:
    sql = """
    select
        * exclude (address_concat, postcode),
        trim(address_concat) as address_concat,
        trim(postcode) as postcode
    from {input}
    """
    step = CTEStep("1", sql)
    return Stage(
        name="trim_whitespace_address_and_postcode",
        steps=[step],
    )
