"""Column-level lineage: source → mapping → target, plus impact analysis.

Lineage is derived from the registered mapping specs, so it is always in
sync with what the engine actually does. Impact analysis answers the
question a change review asks: "if this source column changes, which
target columns and which downstream jobs are affected?"
"""
from __future__ import annotations

from dataclasses import dataclass, field

from dint.models import MappingSpec


@dataclass
class LineageEdge:
    source_connector: str
    source_column: str
    target_table: str
    target_column: str
    transform: str


@dataclass
class ImpactReport:
    source_connector: str
    source_column: str
    affected_targets: list[LineageEdge] = field(default_factory=list)
    affected_jobs: list[str] = field(default_factory=list)  # connector ids


class LineageGraph:
    def __init__(self):
        self._edges: list[LineageEdge] = []

    def register(self, spec: MappingSpec) -> None:
        """(Re)register a mapping spec's edges, replacing its old ones."""
        self._edges = [e for e in self._edges
                       if e.source_connector != spec.connector_id]
        for mapping in spec.mappings:
            self._edges.append(LineageEdge(
                source_connector=spec.connector_id,
                source_column=mapping.source,
                target_table=spec.target_table,
                target_column=mapping.target,
                transform=mapping.transform.value))

    def edges(self) -> list[LineageEdge]:
        return list(self._edges)

    def edges_for_target(self, target_table: str,
                         target_column: str) -> list[LineageEdge]:
        return [e for e in self._edges
                if e.target_table == target_table
                and e.target_column == target_column]

    def impact(self, source_connector: str, source_column: str) -> ImpactReport:
        affected = [e for e in self._edges
                    if e.source_connector == source_connector
                    and e.source_column == source_column]
        jobs = sorted({e.source_connector for e in affected})
        return ImpactReport(source_connector=source_connector,
                            source_column=source_column,
                            affected_targets=affected, affected_jobs=jobs)

    def targets_for_connector(self, connector_id: str) -> list[str]:
        return sorted({e.target_table for e in self._edges
                       if e.source_connector == connector_id})
