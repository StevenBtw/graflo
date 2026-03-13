"""Backward import shim for bindings models.

Internal code should import these classes from ``graflo.architecture.bindings``.
"""

from graflo.architecture.bindings import (
    Bindings,
    FileConnector,
    JoinClause,
    ResourceConnector,
    ResourceType,
    SparqlConnector,
    TableConnector,
)

__all__ = [
    "Bindings",
    "FileConnector",
    "JoinClause",
    "ResourceConnector",
    "ResourceType",
    "SparqlConnector",
    "TableConnector",
]
