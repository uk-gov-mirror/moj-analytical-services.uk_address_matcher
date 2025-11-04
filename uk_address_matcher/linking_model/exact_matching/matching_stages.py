from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable, Literal, Optional, Sequence

from uk_address_matcher.linking_model.exact_matching.annotate_exact_matches import (
    _annotate_exact_matches,
)
from uk_address_matcher.linking_model.exact_matching.input_filters import (
    _filter_unmatched_exact_matches,
    _restrict_canonical_to_fuzzy_postcodes,
)
from uk_address_matcher.linking_model.exact_matching.resolve_with_trie import (
    _resolve_with_trie,
)

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.steps import Stage


FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]
StageFactory = Callable[[FuzzyInputName], "Stage"]


@dataclass(frozen=True)
class ExactMatchStageConfig:
    """A basic stage configuration for exact matching pipeline stages.

    Parameters
    ----------
    factory:
        The primary factory function that produces the stage. Accepts a `fuzzy_input_name`
        parameter to specify which input table the stage should read from.
    pre_filter_canonical:
        The strategy to determine what canonical addresses are considered for
        matching. See `PostcodeStrategy` for details.
    fuzzy_input_name:
        The placeholder name for the fuzzy input table. Defaults to "fuzzy_addresses".
        Should be set to "unmatched_records" for stages that run after filtering.
    """

    factory: StageFactory
    pre_filter_canonical: Optional[Stage] = None
    fuzzy_input_name: FuzzyInputName = "fuzzy_addresses"

    def to_stages(self) -> list[Stage]:
        """Build the stage queue for this exact matching stage."""
        stages: list[Stage] = []
        if self.pre_filter_canonical is not None:
            stages.append(self.pre_filter_canonical)
        stages.append(self.factory(self.fuzzy_input_name))
        return stages


_STAGE_REGISTRY: dict[str, ExactMatchStageConfig] = {
    "annotate_exact_matches": ExactMatchStageConfig(
        factory=_annotate_exact_matches,
        pre_filter_canonical=_restrict_canonical_to_fuzzy_postcodes(
            "exact", "fuzzy_addresses"
        ),
        fuzzy_input_name="fuzzy_addresses",
    ),
    "resolve_with_trie": ExactMatchStageConfig(
        factory=_resolve_with_trie,
        pre_filter_canonical=_restrict_canonical_to_fuzzy_postcodes(
            "drop_last_char", "fuzzy_addresses"
        ),
        fuzzy_input_name="fuzzy_addresses",
    ),
}


def _normalise_optional_stage_names(
    enabled_stage_names: Optional[Iterable[str]],
) -> list[str]:
    """Validate optional stage configuration while preserving order.

    If enabled_stage_names is None, returns an empty list (no optional stages).
    """

    allowed_optionals = {
        name for name in _STAGE_REGISTRY if name != "annotate_exact_matches"
    }

    if enabled_stage_names is None:
        return []

    normalised: list[str] = []
    seen: set[str] = set()
    for name in enabled_stage_names:
        if name == "annotate_exact_matches":
            raise ValueError(
                "annotate_exact_matches is always enabled and should not be provided"
            )
        if name not in allowed_optionals:
            raise ValueError(f"Unknown exact matching stage: {name}")
        if name in seen:
            raise ValueError(f"Duplicate exact matching stage specified: {name}")
        seen.add(name)
        normalised.append(name)
    return normalised


def _build_stage_queue(
    exact_match_queue: Sequence[ExactMatchStageConfig],
) -> list[Stage]:
    """Build the full stage queue from the provided exact match stage configurations."""

    # Between stages we need to filter out any matched records. This can be done using
    # _filter_unmatched_exact_matches

    num_stages = len(exact_match_queue)
    stages: list[Stage] = []

    for iteration, config in enumerate(exact_match_queue, start=1):
        stages.extend(config.to_stages())
        if iteration < num_stages:  # not the last one
            stages.append(_filter_unmatched_exact_matches())

    return stages


def _finalise_results(
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    matches_union: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Join matches back to original fuzzy table to produce final annotated output.

    Handles precedence if multiple stages matched the same ID (first stage wins).
    """
    # Prepare match results with renamed columns to avoid conflicts
    matched_records = matches_union.select("""
        ukam_address_id,
        resolved_canonical_id,
        canonical_ukam_address_id,
        match_reason
    """)

    # Join matches back to original fuzzy addresses
    fuzzy_with_matches = df_addresses_to_match.join(
        matched_records,
        "ukam_address_id",
        how="left",
    )

    # Reorder our columns to enhance readability
    return fuzzy_with_matches.select("""
        unique_id,
        resolved_canonical_id,
        * EXCLUDE (unique_id, resolved_canonical_id, canonical_ukam_address_id, match_reason),
        canonical_ukam_address_id,
        match_reason
    """)
