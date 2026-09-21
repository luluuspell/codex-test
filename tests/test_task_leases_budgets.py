import tempfile
from pathlib import Path

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry, IdempotencyMode
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import (
    ActionProposal, Entity, RiskClass, TaskBudget, TaskState, new_id,
)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import StaleWorld, WorldModel


class ReadCapability:
    def execute(self, operation, locators):
        return {"content": "ok", "locator": locators[0]}

    def verify(self, operation, result):
        return [{"claim": "read", "status": "pass"}]

    def reconcile(self, operation, locators):
        return True, {"content": "ok", "locator": locators[0], "reconciled": True}


class ReadModel:
    def __init__(self, ref="file_A"):
        self.ref = ref

    def next_action(self, manifest):
        return ActionProposal(new_id("proposal"), "files", "read", (self.ref,))


def registry():
    value = CapabilityRegistry()
    value.register(ActionSpec(
        "files", "read", RiskClass.READ, "read",
        revision_domains=frozenset({"workspace"}),
        idempotency_mode=IdempotencyMode.SAFE_REPEAT,
    ))
    return value


def test_workspace_revision_does_not_create_cross_workspace_false_conflict():
    world = WorldModel()
    world.register(Entity("file_A", "file", "a", "/a.txt"))
    world.register(Entity("file_B", "file", "b", "/b.txt"))
    events = EventStore()
    tasks = TaskRuntime(world, events)
    ops = OperationRuntime(world, events, registry())

    task = tasks.create("read a", ("done",), workspace_id="a")
    refs = ReferentStack()
    refs.push("file_A", "selection")
    manifest = build_manifest(task, world, refs)
    expected_a = manifest.world_revisions["workspace"]

    world.register(Entity("file_B2", "file", "b", "/b2.txt"))
    assert world.workspace_revision("a") == expected_a

    proposal = ActionProposal("p1", "files", "read", ("file_A",))
    op = ops.prepare(task, proposal, expected_revisions={"workspace": expected_a})
    assert op.workspace_id == "a"

    stale_expected = world.workspace_revision("a")
    world.observe(
        "desktop", "selection", {"ref": "file_A"}, "workspace",
        workspace_id="a",
    )
    with pytest.raises(StaleWorld):
        ops.prepare(
            task,
            ActionProposal("p2", "files", "read", ("file_A",)),
            expected_revisions={"workspace": stale_expected},
        )


def test_workspace_revisions_survive_restart_independently():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store = SQLiteStore(db)
        world = WorldModel(persistence=store)
        world.register(Entity("a1", "file", "a", "/a1"))
        world.register(Entity("a2", "file", "a", "/a2"))
        world.register(Entity("b1", "file", "b", "/b1"))
        assert world.workspace_revision("a") == 2
        assert world.workspace_revision("b") == 1
        store.close()

        store2 = SQLiteStore(db)
        restored = store2.load_world_model()
        assert restored.workspace_revision("a") == 2
        assert restored.workspace_revision("b") == 1
        store2.close()


def test_durable_task_lease_is_exclusive_and_generation_fenced():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store = SQLiteStore(db)
        world = WorldModel(persistence=store)
        events = EventStore(persistence=store)
        tasks = TaskRuntime(world, events, persistence=store)
        scheduler = TaskScheduler(tasks)
        task = tasks.create(
            "background work", ("done",), workspace_id="ws",
            lane="background", priority=50,
        )
        scheduler.submit(task.task_id)

        lease1 = scheduler.claim_next("runner-1", lease_seconds=10, now_ts=100.0)
        assert lease1 is not None
        assert lease1.generation == 1

        store2 = SQLiteStore(db)
        world2 = store2.load_world_model()
        events2 = EventStore(persistence=store2)
        tasks2 = TaskRuntime(world2, events2, persistence=store2)
        tasks2.tasks = store2.load_tasks()
        scheduler2 = TaskScheduler.rebuild(tasks2)

        assert scheduler2.claim_next(
            "runner-2", lease_seconds=10, now_ts=101.0
        ) is None

        lease2 = scheduler2.claim_next(
            "runner-2", lease_seconds=10, now_ts=111.0
        )
        assert lease2 is not None
        assert lease2.task_id == task.task_id
        assert lease2.generation == 2

        assert scheduler.renew(
            lease1, lease_seconds=10, now_ts=112.0
        ) is None
        renewed = scheduler2.renew(
            lease2, lease_seconds=10, now_ts=112.0
        )
        assert renewed is not None
        assert renewed.lease_until == 122.0
        assert scheduler2.release(renewed)
        store2.close()
        store.close()


def test_operation_budget_is_durable_and_blocks_second_operation():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store = SQLiteStore(db)
        world = WorldModel(persistence=store)
        world.register(Entity("file_A", "file", "ws", "/a.txt"))
        events = EventStore(persistence=store)
        tasks = TaskRuntime(world, events, persistence=store)
        ops = OperationRuntime(
            world, events, registry(),
            capabilities={"files": ReadCapability()},
            persistence=store,
        )
        task = tasks.create(
            "read with one-op budget", ("done",), workspace_id="ws",
            budget=TaskBudget(max_operations=1),
        )
        tasks.control(task.task_id, TaskState.RUNNING)
        refs = ReferentStack()
        refs.push("file_A", "selection")
        policy = PolicyEngine({
            "ws": WorkspacePolicy(allowed_capabilities=frozenset({"files"}))
        })
        runner = NativeAgentRunner(
            tasks, ops, ReadModel(), policy, require_task_lease=False
        )

        runner.step(task, build_manifest(task, world, refs))
        assert task.budget.operations_started == 1
        assert len(ops.operations) == 1
        store.close()

        store2 = SQLiteStore(db)
        coordinator = RecoveryCoordinator.from_store(
            store2,
            registry=registry(),
            capabilities={"files": ReadCapability()},
        )
        restored_task = coordinator.tasks.tasks[task.task_id]
        assert restored_task.budget.operations_started == 1
        runner2 = NativeAgentRunner(
            coordinator.tasks, coordinator.operations, ReadModel(), policy,
            require_task_lease=False,
        )
        runner2.step(
            restored_task,
            build_manifest(restored_task, coordinator.operations.world, refs),
        )
        assert restored_task.state is TaskState.WAITING
        assert restored_task.wait_reason == "budget:max_operations"
        assert len(coordinator.operations.operations) == 1
        store2.close()


def test_deadline_budget_blocks_before_operation_creation():
    world = WorldModel()
    world.register(Entity("file_A", "file", "ws", "/a.txt"))
    events = EventStore()
    tasks = TaskRuntime(world, events)
    ops = OperationRuntime(
        world, events, registry(), capabilities={"files": ReadCapability()}
    )
    task = tasks.create(
        "expired", ("done",), workspace_id="ws",
        budget=TaskBudget(max_operations=5, deadline_at=0.0),
    )
    task.desired_state = TaskState.RUNNING
    refs = ReferentStack()
    refs.push("file_A", "selection")
    policy = PolicyEngine({
        "ws": WorkspacePolicy(allowed_capabilities=frozenset({"files"}))
    })
    NativeAgentRunner(tasks, ops, ReadModel(), policy).step(
        task, build_manifest(task, world, refs)
    )
    assert task.state is TaskState.WAITING
    assert task.wait_reason == "budget:deadline"
    assert ops.operations == {}
