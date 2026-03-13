from graflo.migrate.models import (
    MigrationOperation,
    OperationType,
    RiskLevel,
    SchemaDiffResult,
)
from graflo.migrate.planner import MigrationPlanner


def test_planner_orders_operations_deterministically():
    diff_result = SchemaDiffResult(
        operations=[
            MigrationOperation(
                op_type=OperationType.ADD_EDGE_INDEX,
                target="edge:a:index:1",
                risk=RiskLevel.LOW,
            ),
            MigrationOperation(
                op_type=OperationType.ADD_VERTEX,
                target="vertex:person",
                risk=RiskLevel.LOW,
            ),
            MigrationOperation(
                op_type=OperationType.ADD_EDGE,
                target="edge:person_company",
                risk=RiskLevel.LOW,
            ),
        ]
    )
    plan = MigrationPlanner().build(diff_result)
    assert [op.op_type for op in plan.operations] == [
        OperationType.ADD_VERTEX,
        OperationType.ADD_EDGE,
        OperationType.ADD_EDGE_INDEX,
    ]


def test_planner_blocks_high_risk_by_default():
    diff_result = SchemaDiffResult(
        operations=[
            MigrationOperation(
                op_type=OperationType.REMOVE_VERTEX_FIELD,
                target="vertex:person:field:name",
                risk=RiskLevel.HIGH,
            ),
            MigrationOperation(
                op_type=OperationType.ADD_VERTEX_FIELD,
                target="vertex:person:field:age",
                risk=RiskLevel.LOW,
            ),
        ]
    )
    plan = MigrationPlanner(allow_high_risk=False).build(diff_result)
    assert len(plan.operations) == 1
    assert len(plan.blocked_operations) == 1
    assert plan.blocked_operations[0].op_type == OperationType.REMOVE_VERTEX_FIELD


def test_planner_keeps_high_risk_when_allowed():
    diff_result = SchemaDiffResult(
        operations=[
            MigrationOperation(
                op_type=OperationType.REMOVE_VERTEX_FIELD,
                target="vertex:person:field:name",
                risk=RiskLevel.HIGH,
            )
        ]
    )
    plan = MigrationPlanner(allow_high_risk=True).build(diff_result)
    assert len(plan.operations) == 1
    assert len(plan.blocked_operations) == 0
