import logging

import pytest

from graflo.architecture.edge import EdgeConfig
from graflo.architecture.resource import Resource, _resolve_type_caster
from graflo.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


def test_schema_tree(schema):
    sch = schema("kg")
    mn = Resource.from_dict(sch["resources"][0])
    assert mn.count() == 14


def test_resolve_type_caster_allowlist():
    assert _resolve_type_caster("int") is int
    assert _resolve_type_caster("float") is float
    assert _resolve_type_caster("builtins.str") is str


def test_resolve_type_caster_rejects_expressions():
    assert _resolve_type_caster("__import__('os').system") is None


def test_resource_types_uses_safe_caster_resolution():
    resource = Resource.from_dict(
        {
            "resource_name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "types": {"age": "int", "unsafe": "__import__('os').system"},
        }
    )
    assert resource._types["age"] is int
    assert "unsafe" not in resource._types


def test_resource_infer_edge_selectors_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        Resource.from_dict(
            {
                "resource_name": "typed_resource",
                "pipeline": [{"vertex": "person"}],
                "infer_edge_only": [{"source": "a", "target": "b"}],
                "infer_edge_except": [{"source": "a", "target": "c"}],
            }
        )


def test_resource_infer_edge_selector_references_unknown_edge():
    resource = Resource.from_dict(
        {
            "resource_name": "typed_resource",
            "pipeline": [{"vertex": "person"}],
            "infer_edge_only": [{"source": "a", "target": "b"}],
        }
    )
    vc = VertexConfig.from_dict(
        {"vertices": [{"name": "person", "fields": ["id"], "identity": ["id"]}]}
    )
    ec = EdgeConfig.from_dict({"edges": [{"source": "person", "target": "person"}]})
    with pytest.raises(ValueError, match="unknown edge selectors"):
        resource.finish_init(vertex_config=vc, edge_config=ec, transforms={})
