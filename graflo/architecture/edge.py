"""Edge configuration and management for graph databases.

This module provides classes and utilities for managing edges in graph databases.
It handles edge configuration, weight management, indexing, and relationship operations.
The module supports both ArangoDB and Neo4j through the DBType enum.

Key Components:
    - EdgeBase: Shared base for edge-like configs (Edge and EdgeActorConfig)
    - Edge: Represents an edge with its source, target, and configuration
    - EdgeConfig: Manages collections of edges and their configurations
    - WeightConfig: Configuration for edge weights and relationships

Example:
    >>> edge = Edge(source="user", target="post")
    >>> config = EdgeConfig(edges=[edge])
    >>> edge.finish_init(vertex_config=vertex_config)
"""

from __future__ import annotations

from typing import Any

from pydantic import (
    Field as PydanticField,
    PrivateAttr,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.database_features import DatabaseFeatures
from graflo.architecture.onto import (
    EdgeId,
    EdgeType,
    Index,
    Weight,
)
from graflo.architecture.vertex import Field, FieldType, VertexConfig
from graflo.onto import DBType


# Default relation name for TigerGraph edges when relation is not specified
DEFAULT_TIGERGRAPH_RELATION = "relates"

# Default field name for storing extracted relations in TigerGraph weights
DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME = "relation"


def _normalize_direct_item(item: str | Field | dict[str, Any]) -> Field:
    """Convert a single direct field item (str, Field, or dict) to Field."""
    if isinstance(item, Field):
        return item
    if isinstance(item, str):
        return Field(name=item, type=None)
    if isinstance(item, dict):
        name = item.get("name")
        if name is None:
            raise ValueError(f"Field dict must have 'name' key: {item}")
        return Field(name=name, type=item.get("type"))
    raise TypeError(f"Field must be str, Field, or dict, got {type(item)}")


class WeightConfig(ConfigBaseModel):
    """Configuration for edge weights and relationships.

    This class manages the configuration of weights and relationships for edges,
    including source and target field mappings.

    Attributes:
        vertices: List of weight configurations
        direct: List of direct field mappings. Can be specified as strings, Field objects, or dicts.
               Will be normalized to Field objects by the validator.
               After initialization, this is always list[Field] (type checker sees this).

    Examples:
        >>> # Backward compatible: list of strings
        >>> wc1 = WeightConfig(direct=["date", "weight"])

        >>> # Typed fields: list of Field objects
        >>> wc2 = WeightConfig(direct=[
        ...     Field(name="date", type="DATETIME"),
        ...     Field(name="weight", type="FLOAT")
        ... ])

        >>> # From dicts (e.g., from YAML/JSON)
        >>> wc3 = WeightConfig(direct=[
        ...     {"name": "date", "type": "DATETIME"},
        ...     {"name": "weight"}  # defaults to None type
        ... ])
    """

    vertices: list[Weight] = PydanticField(
        default_factory=list,
        description="List of weight definitions for vertex-based edge attributes.",
    )
    direct: list[Field] = PydanticField(
        default_factory=list,
        description="Direct edge attributes (field names, Field objects, or dicts). Normalized to Field objects.",
    )

    @field_validator("direct", mode="before")
    @classmethod
    def normalize_direct(cls, v: Any) -> Any:
        if not isinstance(v, list):
            return v
        return [_normalize_direct_item(item) for item in v]

    @property
    def direct_names(self) -> list[str]:
        """Get list of direct field names (as strings).

        Returns:
            list[str]: List of field names
        """
        return [field.name for field in self.direct]


class EdgeBase(ConfigBaseModel):
    """Shared base for edge-like configs (Edge schema and EdgeActorConfig).

    Holds the common scalar fields so Edge and EdgeActorConfig stay in sync
    without duplication.
    """

    source: str = PydanticField(
        ...,
        description="Source vertex type name (e.g. user, company).",
    )
    target: str = PydanticField(
        ...,
        description="Target vertex type name (e.g. post, company).",
    )
    match_source: str | None = PydanticField(
        default=None,
        description="Field used to match source vertices when creating edges.",
    )
    match_target: str | None = PydanticField(
        default=None,
        description="Field used to match target vertices when creating edges.",
    )
    relation: str | None = PydanticField(
        default=None,
        description="Relation/edge type name (e.g. Neo4j relationship type). For ArangoDB used as weight.",
    )
    relation_field: str | None = PydanticField(
        default=None,
        description="Field name to store or read relation type (e.g. for TigerGraph).",
    )
    relation_from_key: bool = PydanticField(
        default=False,
        description="If True, derive relation value from the location key during ingestion.",
    )
    exclude_source: str | None = PydanticField(
        default=None,
        description="Exclude source vertices matching this field from edge creation.",
    )
    exclude_target: str | None = PydanticField(
        default=None,
        description="Exclude target vertices matching this field from edge creation.",
    )
    match: str | None = PydanticField(
        default=None,
        description="Match discriminant for edge creation.",
    )


class Edge(EdgeBase):
    """Represents an edge in the graph database.

    An edge connects two vertices and can have various configurations for
    identities, weights, and relationship types.

    Attributes:
        source: Source vertex name
        target: Target vertex name
        identities: Logical candidate identity keys for the edge
        weights: Optional weight configuration
        relation: Optional relation name (for Neo4j)
        match_source: Optional source discriminant field
        match_target: Optional target discriminant field
        type: Edge type (DIRECT or INDIRECT)
        by: Optional vertex name for indirect edges
        graph_name: Optional graph name (ArangoDB only, set in finish_init)
        database_name: Optional database-specific edge identifier (ArangoDB only, set in finish_init).
                       For ArangoDB, this corresponds to the edge collection name.
    """

    identities: list[list[str]] = PydanticField(
        default_factory=list,
        description=(
            "Logical candidate identity keys for this edge. "
            "Each key is a list of identity tokens/fields."
        ),
    )
    weights: WeightConfig | None = PydanticField(
        default=None,
        description="Optional edge weight/attribute configuration (direct fields and vertex-based weights).",
    )

    _relation_dbname: str | None = PrivateAttr(default=None)
    _database_features: DatabaseFeatures | None = PrivateAttr(default=None)
    _store_extracted_relation_as_weight: bool = PrivateAttr(default=False)

    type: EdgeType = PydanticField(
        default=EdgeType.DIRECT,
        description="Edge type: DIRECT (created during ingestion) or INDIRECT (pre-existing collection).",
    )

    by: str | None = PydanticField(
        default=None,
        description="For INDIRECT edges: vertex type name used to define the edge (set to dbname in finish_init).",
    )
    graph_name: str | None = PydanticField(
        default=None,
        description="ArangoDB graph name (set in finish_init).",
    )
    database_name: str | None = PydanticField(
        default=None,
        description="ArangoDB edge collection name (set in finish_init).",
    )

    _source: str | None = PrivateAttr(default=None)
    _target: str | None = PrivateAttr(default=None)

    @field_validator("identities", mode="before")
    @classmethod
    def normalize_identities(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, list):
            # identities can be provided as [["source", "target"], ["source", "target", "pub_id"]]
            if all(isinstance(item, str) for item in v):
                return [list(v)]
            normalized: list[list[str]] = []
            for item in v:
                if isinstance(item, tuple):
                    item = list(item)
                if not isinstance(item, list) or not all(
                    isinstance(token, str) for token in item
                ):
                    raise ValueError("edge identities must be list[list[str]]")
                normalized.append(list(item))
            return normalized
        raise ValueError("edge identities must be list[list[str]]")

    @model_validator(mode="after")
    def normalize_identity_keys(self) -> "Edge":
        deduped_keys: list[list[str]] = []
        seen_keys: set[tuple[str, ...]] = set()
        for key in self.identities:
            deduped_tokens: list[str] = []
            for token in key:
                if token not in deduped_tokens:
                    deduped_tokens.append(token)
            key_tuple = tuple(deduped_tokens)
            if key_tuple and key_tuple not in seen_keys:
                seen_keys.add(key_tuple)
                deduped_keys.append(deduped_tokens)
        object.__setattr__(self, "identities", deduped_keys)
        return self

    @property
    def relation_dbname(self) -> str | None:
        if self._database_features is not None:
            return self._database_features.edge_relation_name(
                self.edge_id,
                default_relation=self.relation,
                logical_relation=self.relation,
            )
        return self._relation_dbname or self.relation

    @relation_dbname.setter
    def relation_dbname(self, value: str | None):
        if self._database_features is not None:
            self._database_features.set_edge_name_spec(
                self.edge_id,
                logical_relation=self.relation,
                relation_name=value,
            )
        self._relation_dbname = value

    @property
    def store_extracted_relation_as_weight(self) -> bool:
        return self._store_extracted_relation_as_weight

    def finish_init(
        self,
        vertex_config: VertexConfig,
        db_flavor: DBType | None = None,
        database_features: DatabaseFeatures | None = None,
    ):
        """Complete edge initialization with vertex configuration.

        Sets up edge collections, graph names, and initializes indices based on
        the vertex configuration.

        Args:
            vertex_config: Configuration for vertices
            db_flavor: Active database flavor
            database_features: DB-only physical features and naming

        """
        if database_features is not None:
            self._database_features = database_features
        if self._database_features is not None and db_flavor is None:
            db_flavor = self._database_features.db_flavor
        if db_flavor is None:
            db_flavor = DBType.ARANGO

        if self.type == EdgeType.INDIRECT and self.by is not None:
            self.by = vertex_config.vertex_dbname(self.by)

        self._source = vertex_config.vertex_dbname(self.source)
        self._target = vertex_config.vertex_dbname(self.target)

        # ArangoDB-specific names are delegated to DatabaseFeatures.
        if self._database_features is not None:
            self.database_name = self._database_features.edge_storage_name(
                self.edge_id,
                source_storage=self._source,
                target_storage=self._target,
            )
            # Graph and edge storage names are intentionally aligned so a purpose
            # uniquely identifies the physical edge variant without redundant naming.
            self.graph_name = self.database_name

        # TigerGraph requires named edge types (relations), so assign default if missing
        if db_flavor == DBType.TIGERGRAPH and self.relation is None:
            # Use default relation name for TigerGraph
            # TigerGraph requires all edges to have a named type (relation)
            self.relation = DEFAULT_TIGERGRAPH_RELATION
            # Ensure dbname follows logical relation by default
            if self.relation_dbname is None:
                self.relation_dbname = self.relation

        # TigerGraph: add relation field to weights if relation_field or relation_from_key is set
        # This ensures the relation value is included as a typed property in the edge schema
        if db_flavor == DBType.TIGERGRAPH:
            self._store_extracted_relation_as_weight = True
            if self.relation_field is None and self.relation_from_key:
                # relation_from_key is True but relation_field not set, default to standard name
                self.relation_field = DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME

            if self.relation_field is not None:
                # Initialize weights if not already present
                if self.weights is None:
                    self.weights = WeightConfig()
                # Type assertion: weights is guaranteed to be WeightConfig after assignment
                assert self.weights is not None, "weights should be initialized"
                # Check if the field already exists in direct weights
                if self.relation_field not in self.weights.direct_names:
                    # Add the relation field with STRING type for TigerGraph
                    self.weights.direct.append(
                        Field(name=self.relation_field, type=FieldType.STRING)
                    )

        else:
            self._store_extracted_relation_as_weight = False

        self._validate_identity_tokens()

    def _validate_identity_tokens(self) -> None:
        """Validate edge identity keys against reserved tokens and declared edge fields."""
        reserved = {"source", "target", "relation"}
        direct_weight_fields = set()
        if self.weights is not None:
            direct_weight_fields = set(self.weights.direct_names)
        relation_field = (
            {self.relation_field} if self.relation_field is not None else set()
        )
        allowed_fields = reserved | direct_weight_fields | relation_field
        unknown_by_key = [
            [token for token in key if token not in allowed_fields]
            for key in self.identities
        ]
        unknown_by_key = [u for u in unknown_by_key if u]
        if unknown_by_key:
            raise ValueError(
                "Edge identity key fields must use reserved tokens "
                "('source', 'target', 'relation') or declared edge direct/relation fields. "
                f"Edge ({self.source}, {self.target}, {self.relation}) has unknown identity fields: {unknown_by_key}"
            )

    @property
    def edge_name_dyad(self):
        """Get the edge name as a dyad (source, target).

        Returns:
            tuple[str, str]: Source and target vertex names
        """
        return self.source, self.target

    @property
    def edge_id(self) -> EdgeId:
        """Alias for edge_id."""
        return self.source, self.target, self.relation


class EdgeConfig(ConfigBaseModel):
    """Configuration for managing collections of edges.

    This class manages a collection of edges, providing methods for accessing
    and manipulating edge configurations.

    Attributes:
        edges: List of edge configurations
    """

    edges: list[Edge] = PydanticField(
        default_factory=list,
        description="List of edge definitions (source, target, identities, weights, relation, etc.).",
    )
    _edges_map: dict[EdgeId, Edge] = PrivateAttr()
    _db_flavor: DBType = PrivateAttr(default=DBType.ARANGO)
    _database_features: DatabaseFeatures | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_edges_map(self) -> EdgeConfig:
        """Build internal mapping of edge IDs to edge configurations."""
        object.__setattr__(self, "_edges_map", {e.edge_id: e for e in self.edges})
        return self

    @staticmethod
    def _map_key(edge: Edge) -> EdgeId:
        return edge.edge_id

    def finish_init(
        self,
        vc: VertexConfig,
        db_flavor: DBType | None = None,
        database_features: DatabaseFeatures | None = None,
    ):
        """Complete initialization of all edges with vertex configuration.

        Args:
            vc: Vertex configuration
            db_flavor: Active database flavor
            database_features: DB-only physical features and naming
        """
        if db_flavor is not None:
            self._db_flavor = db_flavor
        if database_features is not None:
            self._database_features = database_features

        active_db_flavor = db_flavor or self._db_flavor
        active_database_features = database_features or self._database_features

        for e in self.edges:
            e.finish_init(
                vertex_config=vc,
                db_flavor=active_db_flavor,
                database_features=active_database_features,
            )
        if active_database_features is not None:
            self._compile_identity_indexes(
                db_flavor=active_db_flavor,
                database_features=active_database_features,
            )

    def _identity_key_index_fields(
        self, identity_key: list[str], db_flavor: DBType
    ) -> list[str]:
        fields: list[str] = []
        for token in identity_key:
            if token == "source":
                if db_flavor == DBType.ARANGO:
                    fields.append("_from")
            elif token == "target":
                if db_flavor == DBType.ARANGO:
                    fields.append("_to")
            elif token == "relation":
                # Relation is represented as a stored edge field in non-TigerGraph backends.
                # For TigerGraph it is represented via edge type/discriminator handling.
                if db_flavor != DBType.TIGERGRAPH:
                    fields.append("relation")
            else:
                fields.append(token)
        deduped: list[str] = []
        for field in fields:
            if field not in deduped:
                deduped.append(field)
        return deduped

    def _compile_identity_indexes(
        self, *, db_flavor: DBType, database_features: DatabaseFeatures
    ) -> None:
        for edge in self.edges:
            # Empty identities => permissive default (allow multi-edges).
            for identity_key in edge.identities:
                identity_fields = self._identity_key_index_fields(
                    identity_key, db_flavor
                )
                if not identity_fields:
                    continue
                database_features.add_edge_index(
                    edge.edge_id,
                    Index(fields=identity_fields, unique=True),
                    logical_relation=edge.relation,
                    purpose=None,
                )

    def edges_list(self, include_aux: bool = False):
        """Get list of edges.

        Args:
            include_aux: Deprecated. Kept for backward compatibility.

        Returns:
            generator: Generator yielding edge configurations
        """
        _ = include_aux
        return (e for e in self._edges_map.values())

    def edges_items(self, include_aux: bool = False):
        """Get items of edges.

        Args:
            include_aux: Deprecated. Kept for backward compatibility.

        Returns:
            generator: Generator yielding (edge_id, edge) tuples
        """
        _ = include_aux
        return ((e.edge_id, e) for e in self._edges_map.values())

    def __contains__(self, item: EdgeId | EdgeId | Edge):
        """Check if edge exists in configuration.

        Args:
            item: Edge ID or Edge instance to check

        Returns:
            bool: True if edge exists, False otherwise
        """
        if isinstance(item, Edge):
            return self._map_key(item) in self._edges_map
        if isinstance(item, tuple) and len(item) == 3:
            return item in self._edges_map
        return False

    def update_edges(
        self,
        edge: Edge,
        vertex_config: VertexConfig,
        db_flavor: DBType | None = None,
        database_features: DatabaseFeatures | None = None,
    ):
        """Update edge configuration.

        Args:
            edge: Edge configuration to update
            vertex_config: Vertex configuration
        """
        edge_key = self._map_key(edge)
        if edge_key in self._edges_map:
            self._edges_map[edge_key].update(edge)
        else:
            self._edges_map[edge_key] = edge
            self.edges.append(edge)

        active_db_flavor = db_flavor or self._db_flavor
        active_database_features = database_features or self._database_features

        self._edges_map[edge_key].finish_init(
            vertex_config=vertex_config,
            db_flavor=active_db_flavor,
            database_features=active_database_features,
        )

    @property
    def vertices(self):
        """Get set of vertex names involved in edges.

        Returns:
            set[str]: Set of vertex names
        """
        return {e.source for e in self.edges} | {e.target for e in self.edges}
