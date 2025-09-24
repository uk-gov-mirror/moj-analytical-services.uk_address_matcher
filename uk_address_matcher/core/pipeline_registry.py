from __future__ import annotations

import re
from dataclasses import dataclass, field
import typing as T
import logging

from uk_address_matcher.core.pipeline_debug import emit_debug, pretty_sql

if T.TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("uk_address_matcher")


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower())


# Types for enhanced system
SQLReturn = T.Union[str, T.List[T.Tuple[str, str]]]  # "sql" or [(cte_name, sql), ...]


@dataclass(frozen=True)
class StepMeta:
    name: str
    description: str = ""
    group: T.Optional[str] = None
    input_cols: T.List[str] = field(default_factory=list)
    output_cols: T.List[str] = field(default_factory=list)
    depends_on: T.List[str] = field(default_factory=list)
    tags: T.Set[str] = field(default_factory=set)


@dataclass
class Step:
    meta: StepMeta
    fn: T.Callable[[], SQLReturn]
    # registration index for stable ordering when no deps
    reg_index: int


@dataclass
class PipelineDefinition:
    name: str
    description: str = ""
    # canonical, dependency-complete, topo-ordered names
    ordered_steps: T.List[str] = field(default_factory=list)

    def run(
        self,
        *,
        con,  # DuckDBPyConnection
        input_relation,  # Union[DuckDBPyRelation, str]
        input_alias: str = "inputs",
        extra_inputs: T.Optional[T.Dict[str, T.Union[object, str]]] = None,
        options: T.Optional["ExecOptions"] = None,
    ):
        """Execute this pipeline in DuckDB."""
        return run_pipeline(
            pipeline=self.ordered_steps,  # Pass steps directly instead of name
            con=con,
            input_relation=input_relation,
            input_alias=input_alias,
            extra_inputs=extra_inputs,
            options=options,
        )

    def text(self) -> str:
        """Get text representation of this pipeline."""
        return pipeline_text(self.ordered_steps)


class _Registry:
    def __init__(self) -> None:
        self._steps: T.Dict[str, Step] = {}
        self._pipelines: T.Dict[str, PipelineDefinition] = {}
        self._order_counter = 0
        # reverse maps to resolve names/metas from callables without mutating them
        self._fn_to_name: T.Dict[T.Callable, str] = {}
        self._fn_to_meta: T.Dict[T.Callable, StepMeta] = {}

    # --- step API
    def add_step(self, meta: StepMeta, fn: T.Callable[[], SQLReturn]) -> None:
        if meta.name in self._steps:
            logger.warning("Overwriting step '%s'", meta.name)
        self._steps[meta.name] = Step(meta=meta, fn=fn, reg_index=self._order_counter)
        self._order_counter += 1
        # populate reverse lookups
        self._fn_to_name[fn] = meta.name
        self._fn_to_meta[fn] = meta

    def get_step(self, name: str) -> Step:
        if name not in self._steps:
            raise KeyError(f"Step '{name}' is not registered")
        return self._steps[name]

    def has_step(self, name: str) -> bool:
        return name in self._steps

    def all_step_names(self) -> T.List[str]:
        return [
            k for k, _ in sorted(self._steps.items(), key=lambda kv: kv[1].reg_index)
        ]


PIPELINE_REGISTRY = _Registry()


def register_step(
    *,
    name: str,
    description: str = "",
    group: T.Optional[str] = None,
    input_cols: T.Optional[T.List[str]] = None,
    output_cols: T.Optional[T.List[str]] = None,
    depends_on: T.Optional[T.List[str]] = None,
    tags: T.Optional[T.Iterable[str]] = None,
):
    """Decorator to register a SQL step function.

    The function must return either:
      - a SQL string (may reference {input} and/or {input.alias}), or
      - a list of (cte_name, sql) tuples for multi-CTE steps.

    Args:
        name: unique step name. This is how the step is referenced and logged
            in debugging output.
        description: brief description of the step
        group: optional category/group name
        input_cols: optional list of required input columns
        output_cols: optional list of output columns added/modified by this step
        depends_on: optional list of step names this step depends on
        tags: optional set of tags for this step
    """

    def decorator(fn: T.Callable[[], SQLReturn]):
        meta = StepMeta(
            name=name,
            description=description,
            group=group,
            input_cols=list(input_cols or []),
            output_cols=list(output_cols or []),
            depends_on=list(depends_on or []),
            tags=set(tags or []),
        )
        PIPELINE_REGISTRY.add_step(meta, fn)
        return fn

    return decorator


def _resolve_step_name(s: T.Union[str, T.Callable]) -> str:
    if isinstance(s, str):
        return s
    # Prefer reverse registry map (no mutation of callables)
    try:
        return PIPELINE_REGISTRY._fn_to_name[s]  # type: ignore[index]
    except Exception:
        # Back-compat: support older decorated functions that used setattr
        n = getattr(s, "_step_name", None)
        if not n:
            raise ValueError(
                "All steps must be registered, or pass step names as strings"
            )
        return n


def create_pipeline(
    *,
    name: str,
    steps: T.List[T.Union[str, T.Callable]],  # names or decorated functions
    description: str = "",
) -> PipelineDefinition:
    sel = [_resolve_step_name(s) for s in steps]
    return PipelineDefinition(name=name, description=description, ordered_steps=sel)


@dataclass
class ExecOptions:
    debug: bool = False  # show per-step SQL and a few rows
    debug_rows: int = 10  # rows to show when debug
    echo_final_sql: bool = False  # pretty-print final SQL
    collect_cols: bool = True  # use declared output_cols to update known cols


_INPUT_BRACE = re.compile(r"\{input(?:\.([a-zA-Z_][a-zA-Z0-9_]*))?\}")


def _render_sql(sql: str, current_input: str, input_aliases: T.Dict[str, str]) -> str:
    """Replace {input} and {input.alias} placeholders."""

    def repl(m: re.Match) -> str:
        alias = m.group(1)
        if alias is None:
            return current_input
        if alias not in input_aliases:
            raise KeyError(f"Unknown input alias '{{input.{alias}}}'")
        return input_aliases[alias]

    return _INPUT_BRACE.sub(repl, sql)


def build_sql_for_steps(
    step_names: T.List[str],
    *,
    default_input_alias: str = "inputs",
    input_aliases: T.Optional[T.Dict[str, str]] = None,
) -> T.Tuple[str, T.List[T.Tuple[str, str]]]:
    """Create the WITH-chain SQL and the list of concrete CTEs (sql, alias)."""
    input_aliases = dict(input_aliases or {})
    if "default" not in input_aliases:
        input_aliases["default"] = default_input_alias

    ctes: T.List[T.Tuple[str, str]] = []
    current = default_input_alias

    for index, name in enumerate(step_names):
        step = PIPELINE_REGISTRY.get_step(name)
        raw = step.fn()

        if isinstance(raw, str):
            alias = f"s{index}_{_slug(name)}"
            sql = _render_sql(raw, current, input_aliases)
            ctes.append((sql, alias))
            current = alias
        else:
            last_alias: T.Optional[str] = None
            for j, (cte_name, frag_sql) in enumerate(raw):
                alias = f"s{index}_{_slug(name)}__{_slug(cte_name)}"
                sql = _render_sql(frag_sql, current, input_aliases)
                ctes.append((sql, alias))
                current = alias
                last_alias = alias
            if not last_alias:
                raise ValueError(f"Step '{name}' returned empty CTE list")

    if not ctes:
        raise ValueError("No steps to build")

    with_chain = ",\n\n".join(f"{alias} AS (\n{sql}\n)" for (sql, alias) in ctes)
    final_sql = f"WITH\n{with_chain}\n\nSELECT * FROM {ctes[-1][1]}"
    return final_sql, ctes


def validate_initial_inputs(
    step_order: T.List[str], initial_cols: T.Set[str]
) -> T.List[str]:
    """Check that the first step's inputs are present (and optionally propagate)."""
    problems: T.List[str] = []
    known = set(initial_cols)

    for name in step_order:
        meta = PIPELINE_REGISTRY.get_step(name).meta
        missing = set(meta.input_cols or []) - known
        if missing:
            problems.append(f"Step '{name}' missing input cols: {sorted(missing)}")
    for c in meta.output_cols or []:
        known.add(c)
    return problems


def run_pipeline(
    *,
    pipeline: T.Union[str, T.List[T.Union[str, T.Callable]]],
    con,  # DuckDBPyConnection
    input_relation,  # Union[DuckDBPyRelation, str]
    input_alias: str = "inputs",
    extra_inputs: T.Optional[T.Dict[str, T.Union[object, str]]] = None,
    options: T.Optional[ExecOptions] = None,
):
    """Compile and execute a pipeline in DuckDB (enhanced system)."""
    if duckdb is None:
        raise RuntimeError("duckdb is not available")

    options = options or ExecOptions()
    if isinstance(pipeline, str):
        pl = PIPELINE_REGISTRY.get_pipeline(pipeline)
        step_order = pl.ordered_steps
    else:
        step_order = [_resolve_step_name(s) for s in pipeline]

    if isinstance(input_relation, str):
        base_name = input_relation
    else:
        base_name = input_alias
        con.register(base_name, input_relation)

    input_aliases: T.Dict[str, str] = {"default": base_name, "base": base_name}
    if extra_inputs:
        for k, v in extra_inputs.items():
            if isinstance(v, str):
                input_aliases[k] = v
            else:
                con.register(k, v)
                input_aliases[k] = k

    try:
        initial_cols = set(con.sql(f"SELECT * FROM {base_name} LIMIT 0").columns)
        problems = validate_initial_inputs(step_order, initial_cols)
        if problems:
            logger.warning("\n".join(problems))
    except Exception as e:
        logger.debug("Skipping initial column validation: %s", e)

    final_sql, ctes = build_sql_for_steps(
        step_order, default_input_alias=base_name, input_aliases=input_aliases
    )

    if options.debug:
        from .pipeline_debug import debug_steps

        debug_steps(
            con=con,
            ctes=ctes,
            step_order=step_order,
            options=options,
            get_step=lambda n: PIPELINE_REGISTRY.get_step(n),
        )

    if options.echo_final_sql:
        emit_debug("\n===== FINAL SQL =====\n")
        emit_debug(pretty_sql(final_sql))
        emit_debug("\n=====================\n")

    return con.sql(final_sql)


def pipeline_text(pipeline: T.Union[str, T.List[T.Union[str, T.Callable]]]) -> str:
    if isinstance(pipeline, str):
        names = PIPELINE_REGISTRY.get_pipeline(pipeline).ordered_steps
        title = f"Pipeline: {pipeline}"
    else:
        names = [_resolve_step_name(s) for s in pipeline]
        title = "Ad-hoc pipeline"

    lines = [title, "=" * len(title), ""]

    for i, step_name in enumerate(names, 1):
        meta = PIPELINE_REGISTRY.get_step(step_name).meta

        step_parts = [f"{i:2d}."]
        display_name = step_name if len(step_name) <= 35 else step_name[:32] + "..."
        step_parts.append(display_name)
        if meta.group:
            step_parts.append(f"[{meta.group}]")

        main_line = " ".join(step_parts)
        lines.append(main_line)

        if meta.description:
            lines.append(f"    ↳ {meta.description}")

        io_parts = []
        if meta.input_cols:
            inputs = ", ".join(meta.input_cols)
            if len(inputs) > 40:
                inputs = inputs[:37] + "..."
            io_parts.append(f"in: {inputs}")

        if meta.output_cols:
            outputs = ", ".join(meta.output_cols)
            if len(outputs) > 40:
                outputs = outputs[:37] + "..."
            io_parts.append(f"out: {outputs}")

        if io_parts:
            lines.append(f"    │ {' | '.join(io_parts)}")

        if meta.depends_on:
            deps = ", ".join(meta.depends_on)
            if len(deps) > 50:
                deps = deps[:47] + "..."
            lines.append(f"    │ depends on: {deps}")

        if i < len(names):
            lines.append("")

    return "\n".join(lines)
