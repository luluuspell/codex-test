import tempfile
from pathlib import Path

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import (
    ActionSpec, CapabilityRegistry, IdempotencyMode, InvalidAction,
)
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import (
    ActionProposal, Entity, OperationState, RiskClass, TaskState, new_id,
)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


class Capability:
    def __init__(self, fail=False):
        self.fail = fail

    def execute(self, operation, locators):
        if self.fail:
            raise TimeoutError("lost response")
        return {"ok": True, "locator": locators[0]}

    def verify(self, operation, result):
        return [{"claim": "ok", "status": "pass" if result.get("ok") else "fail"}]

    def reconcile(self, operation, locators):
        return True, {"ok": True, "locator": locators[0], "reconciled": True}


class Model:
    def __init__(self, action="read", arguments=None):
        self.action = action
        self.arguments = arguments or {}

    def next_action(self, manifest):
        return ActionProposal(
            new_id("proposal"), "files", self.action, ("file_A",), self.arguments
        )


def registry():
    result = CapabilityRegistry()
    result.register(ActionSpec(
        "files", "read", RiskClass.READ, "read",
        allowed_arguments=frozenset({"offset", "length"}),
        revision_domains=frozenset({"workspace"}),
        idempotency_mode=IdempotencyMode.SAFE_REPEAT,
    ))
    result.register(ActionSpec(
        "files", "write", RiskClass.MUTATING, "write",
        allowed_arguments=frozenset({"content"}),
        required_arguments=frozenset({"content"}),
        revision_domains=frozenset({"workspace"}),
    ))
    return result


def build_persistent_core(path):
    store = SQLiteStore(path)
    world = WorldModel(persistence=store)
    world.register(Entity(
        "file_A", "file", "ws", "/workspace/a.txt",
        permissions=frozenset({"read", "write"}),
    ))
    events = EventStore(persistence=store)
    tasks = TaskRuntime(world, events, persistence=store)
    ops = OperationRuntime(world, events, registry(), persistence=store)
    return world, events, store, tasks, ops


def test_restart_hydrates_world_task_operation_and_reconciles_unknown():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        world, events, store, tasks, ops = build_persistent_core(db)
        failing = Capability(fail=True)
        ops.capabilities["files"] = failing
        task = tasks.create("read", ("read_done",))
        task.desired_state = TaskState.RUNNING
        task.state = TaskState.RUNNING
        store.save_task(task)
        proposal = ActionProposal("p", "files", "read", ("file_A",))
        op = ops.prepare(
            task, proposal,
            expected_revisions={"workspace": world.revisions.workspace},
        )
        ops.execute(op)
        assert op.state is OperationState.UNKNOWN
        events.flush_outbox()
        store.close()

        store2 = SQLiteStore(db)
        coordinator = RecoveryCoordinator.from_store(
            store2, registry=registry(), capabilities={"files": Capability()}
        )
        assert coordinator.operations.world.get("file_A").locator == "/workspace/a.txt"
        result = coordinator.recover()
        loaded = coordinator.operations.operations[op.operation_id]
        assert result["operations_reconciled"] == 1
        assert loaded.state is OperationState.VERIFIED
        durable_events = coordinator.operations.events.read_after("audit")
        assert any(e.event_type == "operation.reconciled" for e in durable_events)
        store2.close()


def test_durable_event_cursor_survives_restart():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        world, events, store, tasks, ops = build_persistent_core(db)
        tasks.create("x", ("done",))
        emitted = events.flush_outbox()
        assert emitted
        events.ack("memory", emitted[-1].sequence)
        store.close()

        store2 = SQLiteStore(db)
        events2 = EventStore(persistence=store2)
        assert events2.read_after("memory") == []
        store2.close()


def test_policy_uses_authoritative_action_risk_before_operation_creation():
    with tempfile.TemporaryDirectory() as td:
        world, events, store, tasks, ops = build_persistent_core(Path(td) / "native.db")
        ops.capabilities["files"] = Capability()
        refs = ReferentStack()
        refs.push("file_A", "selection")
        policy = PolicyEngine({"ws": WorkspacePolicy(
            allowed_capabilities=frozenset({"files"})
        )})

        read_task = tasks.create("read", ("done",))
        read_task.desired_state = TaskState.RUNNING
        NativeAgentRunner(
            tasks, ops, Model("read"), policy, workspace_id="ws"
        ).step(read_task, build_manifest(read_task, world.revisions, refs))
        assert len(ops.operations) == 1

        write_task = tasks.create("write", ("done",))
        write_task.desired_state = TaskState.RUNNING
        before = len(ops.operations)
        NativeAgentRunner(
            tasks, ops, Model("write", {"content": "x"}), policy, workspace_id="ws"
        ).step(write_task, build_manifest(write_task, world.revisions, refs))
        assert write_task.state is TaskState.WAITING
        assert len(ops.operations) == before
        store.close()


def test_action_schema_blocks_path_smuggling():
    proposal = ActionProposal(
        "p", "files", "read", ("file_A",),
        {"path": "/Users/secret.txt"},
    )
    with pytest.raises(InvalidAction):
        registry().resolve(proposal)


def test_cancel_request_does_not_claim_actual_cancel_until_safe_boundary():
    with tempfile.TemporaryDirectory() as td:
        world, events, store, tasks, ops = build_persistent_core(Path(td) / "native.db")
        task = tasks.create("long", ("done",))
        task.state = TaskState.RUNNING
        task.desired_state = TaskState.RUNNING
        store.save_task(task)
        tasks.control(task.task_id, TaskState.CANCELLED)
        assert task.desired_state is TaskState.CANCELLED
        assert task.state is TaskState.CANCELLING
        tasks.settle_control(task.task_id, safe_boundary=True)
        assert task.state is TaskState.CANCELLED
        store.close()
