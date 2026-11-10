"""Lineage tests: edges from mappings, target lookup, impact analysis."""
from dint.lineage import LineageGraph
from dint.models import FieldMapping, MappingSpec, TransformKind


def _spec(connector_id="orders-file"):
    return MappingSpec(connector_id, "orders", [
        FieldMapping("order_id", "order_id"),
        FieldMapping("region_code", "region", TransformKind.LOOKUP,
                     {"table": {"north": "North"}, "default": "Unknown"}),
        FieldMapping("qty", "quantity", TransformKind.CAST, {"to": "int"}),
    ])


def test_register_creates_edges():
    graph = LineageGraph()
    graph.register(_spec())
    assert len(graph.edges()) == 3
    assert graph.edges_for_target("orders", "region")[0].source_column == "region_code"
    assert graph.edges_for_target("orders", "region")[0].transform == "lookup"


def test_reregister_replaces_edges():
    graph = LineageGraph()
    graph.register(_spec())
    graph.register(MappingSpec("orders-file", "orders",
                               [FieldMapping("order_id", "order_id")]))
    assert len(graph.edges()) == 1


def test_impact_analysis():
    graph = LineageGraph()
    graph.register(_spec())
    report = graph.impact("orders-file", "region_code")
    assert len(report.affected_targets) == 1
    assert report.affected_targets[0].target_column == "region"
    assert report.affected_jobs == ["orders-file"]


def test_impact_unknown_column_is_empty():
    graph = LineageGraph()
    graph.register(_spec())
    report = graph.impact("orders-file", "nope")
    assert report.affected_targets == []
    assert report.affected_jobs == []


def test_targets_for_connector():
    graph = LineageGraph()
    graph.register(_spec())
    graph.register(MappingSpec("customers-api", "customers",
                               [FieldMapping("customer_id", "customer_id")]))
    assert graph.targets_for_connector("orders-file") == ["orders"]
    assert graph.targets_for_connector("customers-api") == ["customers"]
