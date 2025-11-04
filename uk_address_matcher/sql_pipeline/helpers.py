import logging
import random
import re
import string
from dataclasses import dataclass, field
from typing import List

import duckdb

logger = logging.getLogger("uk_address_matcher")


@dataclass
class StageTimingRecord:
    """Records execution timing for a pipeline stage."""

    step_number: int
    alias: str
    stage_name: str | None
    fragment_name: str | None
    duration_seconds: float


@dataclass
class TimingReport:
    """Collects and formats timing information for pipeline stages."""

    records: List[StageTimingRecord] = field(default_factory=list)

    def add_timing(
        self,
        step_number: int,
        alias: str,
        duration_seconds: float,
        stage_name: str | None = None,
        fragment_name: str | None = None,
    ) -> None:
        """Record a stage execution time."""
        self.records.append(
            StageTimingRecord(
                step_number=step_number,
                alias=alias,
                stage_name=stage_name,
                fragment_name=fragment_name,
                duration_seconds=duration_seconds,
            )
        )

    def format_report(self) -> str:
        """Generate a formatted timing report."""
        if not self.records:
            return "(no timing data collected)"

        lines = ["\n" + "=" * 80]
        lines.append("⏱️  PIPELINE TIMING REPORT")
        lines.append("=" * 80)

        total_duration = sum(r.duration_seconds for r in self.records)

        # Column headers
        lines.append(
            f"{'Step':<6} {'Duration':<12} {'%':<7} {'Alias':<25} {'Stage / Fragment'}"
        )
        lines.append("-" * 80)

        for record in self.records:
            percentage = (
                (record.duration_seconds / total_duration * 100)
                if total_duration > 0
                else 0
            )
            duration_str = _format_duration(record.duration_seconds)

            stage_info = ""
            if record.stage_name and record.fragment_name:
                stage_info = f"{record.stage_name} / {record.fragment_name}"
            elif record.stage_name:
                stage_info = record.stage_name
            elif record.fragment_name:
                stage_info = record.fragment_name

            lines.append(
                f"{record.step_number:<6} {duration_str:<12} {percentage:>5.1f}%  "
                f"{record.alias:<25} {stage_info}"
            )

        lines.append("-" * 80)
        lines.append(f"{'TOTAL':<6} {_format_duration(total_duration):<12}")
        lines.append("=" * 80 + "\n")

        return "\n".join(lines)


def _format_duration(seconds: float) -> str:
    if seconds >= 60:
        minutes, rem = divmod(seconds, 60)
        return f"{int(minutes)}m {rem:05.2f}s"
    return f"{seconds:.2f}s"


def _emit_debug(msg: str) -> None:
    """Emit debug output via logger if configured, else stdout.

    Many users won't configure logging in quick scripts, so when debug/pretty-print
    is enabled we print to stdout to ensure visibility.
    """
    if logger.handlers and logger.isEnabledFor(logging.DEBUG):
        logger.debug(msg)
    else:
        print(msg)


def _uid(n: int = 6) -> str:
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(n)
    )


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower())


def _pretty_sql(sql: str) -> str:
    # Hook for pretty printers if desired
    return sql


def _duckdb_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    ).fetchone()
    return result[0] > 0


def _explain_debug(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    _emit_debug(
        f"Generating EXPLAIN plan for final SQL.\n============================\n:{sql}\n"
    )
    _emit_debug(con.sql(f"EXPLAIN {sql}").fetchone()[1])
