from __future__ import annotations

import logging
from typing import Callable, Dict, List, Tuple


logger = logging.getLogger("uk_address_matcher")


def pretty_sql(sql: str) -> str:
    """Hook to pretty print SQL; placeholder returns the input string."""
    return sql


def debug_steps(
    *,
    con,
    ctes: List[Tuple[str, str]],
    step_order: List[str],
    options,
    get_step: Callable[[str], object],
) -> None:
    """Show per-CTE debug information and preview results.

    - con: DuckDB connection
    - ctes: list of (sql, alias) built for the pipeline
    - step_order: ordered step names
    - options: object with attribute `debug_rows`
    - get_step: function to retrieve step definition by name; must return an
      object with `.meta.description` and `.meta.group` and `.fn()` for multi-CTE mapping.
    """
    logger.debug("\n🐞 Step-by-step debugging:")
    logger.debug("=" * 60)

    # Map each CTE index to the parent step info for nicer headers
    step_info_map: Dict[int, Dict[str, str]] = {}
    cte_idx = 0
    for step_name in step_order:
        step_def = get_step(step_name)
        raw = step_def.fn()
        if isinstance(raw, str):
            step_info_map[cte_idx] = {
                "step_name": step_name,
                "step_description": step_def.meta.description,
                "step_group": step_def.meta.group,
                "cte_name": "",
            }
            cte_idx += 1
        else:
            for cte_name, _ in raw:
                step_info_map[cte_idx] = {
                    "step_name": step_name,
                    "step_description": step_def.meta.description,
                    "step_group": step_def.meta.group,
                    "cte_name": cte_name,
                }
                cte_idx += 1

    for idx, (sql, alias) in enumerate(ctes, 1):
        info = step_info_map.get(idx - 1, {})
        step_name = info.get("step_name", "unknown")
        description = info.get("step_description", "")
        group = info.get("step_group", "")
        cte_name = info.get("cte_name", "")

        header_parts = [f"STEP {idx}/{len(ctes)}"]
        if group:
            header_parts.append(f"[{group}]")
        header_parts.append(step_name)
        if cte_name:
            header_parts.append(f"→ {cte_name}")

        header = " ".join(header_parts)
        logger.debug(f"\n--- {header} ---")
        if description:
            logger.debug(f"    ↳ {description}")
        logger.debug(f"    alias: {alias}")
        logger.debug(f"\n{sql}\n")

        try:
            partial_ctes = ctes[:idx]
            if len(partial_ctes) == 1:
                debug_sql = f"WITH {alias} AS (\n{sql}\n) SELECT * FROM {alias} LIMIT {options.debug_rows}"
            else:
                with_parts = []
                for s, a in partial_ctes:
                    with_parts.append(f"{a} AS (\n{s}\n)")
                with_chain = ",\n\n".join(with_parts)
                debug_sql = f"WITH\n{with_chain}\n\nSELECT * FROM {alias} LIMIT {options.debug_rows}"
            con.sql(debug_sql).show()
        except Exception as e:  # pragma: no cover - debug path
            logger.debug(f"(could not preview: {e})")
