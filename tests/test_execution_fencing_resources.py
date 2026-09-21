import tempfile
import threading
from pathlib import Path

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry, IdempotencyMode
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.memory import MemoryCandidate, MemoryKind
from liandanlu_native.models import (
    ActionProposal, Entity, ResourceCapacity, ResourceRequest, RiskClass,
    TaskState, new_id, now,
)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.resources import ResourceBroker
from liandanlu_native.runtime import (
    LeaseLost, OperationRuntime, TaskRuntime, TaskScheduler,
)
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


class CountingCapability:
    def __init__(self):
        self.calls = 0

    def execute(self, operation, locators):
        self.calls += 1
        return {"ok": True, "locator": locators[0]}

    def verify(self, operation, result):
        return [{"claim": "ok", "status": "pass"}]

    def reconcile(self, operation, locators):
        return True, {"ok": True, "locator": locators[0], "reconciled": True}


class Model:
    def next_action(self, manifest):
        return ActionProposal(new_id("proposal"), "heavy", "run", ("file_A",))


def heavy_registry(request: ResourceRequest | None = None):
    registry = CapabilityRegistry()
    registry.register(ActionSpec(
        "heavy",
        "run",
        RiskClass.READ,
        "read",
        revision_domains=frozenset({"workspace"}),
        resource_request=request or ResourceRequest(),
        idempotency_mode=IdempotencyMode.RECONCILABLE,
    ))
    return registry


def build_core(db: Path, *, request: ResourceRequest | None = None):
    store = SQLiteStore(db)
    world = WorldModel(persistence=store)
    world.register(Entity("file_A", "file", "ws", "/workspace/a.txt"))
    events = EventStore(persistence=store)
    tasks = TaskRuntime(world, events, persistence=store)
    capability = CountingCapability()
    operations = OperationRuntime(
        world,
        events,
        heavy_registry(request),
        capabilities={"heavy": capability},
        persistence=store,
    )
    return store, world, events, tasks, operations, capability


def test_stale_task_lease_fence_blocks_side_effect_execution():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db"
        )
        scheduler = TaskScheduler(tasks)
        task = tasks.create("work", ("done",), workspace_id="ws")
        scheduler.submit(task.task_id)

        base = now()
        lease1 = scheduler.claim_next("runner-1", lease_seconds=1.0, now_ts=base)
        assert lease1 is not None
        proposal = ActionProposal("p", "heavy", "run", ("file_A",))
        op = ops.prepare(
            task,
            proposal,
            expected_revisions={"workspace": world.workspace_revision("ws")},
            task_lease=lease1,
        )
        lease2 = scheduler.claim_next(
            "runner-2", lease_seconds=30.0, now_ts=base + 2.0
        )
        assert lease2 is not None and lease2.generation == lease1.generation + 1

        with pytest.raises(LeaseLost):
            ops.execute(op)
        assert capability.calls == 0
        assert op.state.name == "PREPARED"
        store.close()


def test_resource_broker_prevents_capacity_overcommit_and_exclusive_collision():
    with tempfile.TemporaryDirectory() as td:
        store = SQLiteStore(Path(td) / "native.db")
        broker = ResourceBroker(
            ResourceCapacity(cpu_units=100, memory_mb=1000, gpu_units=100),
            persistence=store,
        )
        base = now()
        first = broker.acquire(
            "task-1",
            "runner-1",
            ResourceRequest(
                cpu_units=60,
                memory_mb=600,
                gpu_units=20,
                exclusive_labels=("unified-memory-heavy",),
            ),
            lease_seconds=30,
            now_ts=base,
        )
        assert first is not None
        assert broker.acquire(
            "task-2",
            "runner-2",
            ResourceRequest(cpu_units=50, memory_mb=500),
            lease_seconds=30,
            now_ts=base,
        ) is None
        assert broker.acquire(
            "task-3",
            "runner-3",
            ResourceRequest(exclusive_labels=("unified-memory-heavy",)),
            lease_seconds=30,
            now_ts=base,
        ) is None

        assert broker.release(first)
        second = broker.acquire(
            "task-2",
            "runner-2",
            ResourceRequest(cpu_units=50, memory_mb=500),
            lease_seconds=30,
            now_ts=base,
        )
        assert second is not None
        store.close()


def test_expired_resource_generation_cannot_execute_prepared_operation():
    with tempfile.TemporaryDirectory() as td:
        request = ResourceRequest(memory_mb=600)
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db", request=request
        )
        scheduler = TaskScheduler(tasks)
        task = tasks.create("heavy", ("done",), workspace_id="ws")
        scheduler.submit(task.task_id)
        base = now()
        task_lease = scheduler.claim_next(
            "runner-1", lease_seconds=60, now_ts=base
        )
        assert task_lease is not None

        broker = ResourceBroker(
            ResourceCapacity(cpu_units=100, memory_mb=1000, gpu_units=100),
            persistence=store,
        )
        resource1 = broker.acquire(
            task.task_id,
            task_lease.owner_id,
            request,
            lease_seconds=1,
            now_ts=base,
        )
        assert resource1 is not None
        op = ops.prepare(
            task,
            ActionProposal("p", "heavy", "run", ("file_A",)),
            expected_revisions={"workspace": world.workspace_revision("ws")},
            task_lease=task_lease,
            resource_lease=resource1,
        )
        resource2 = broker.acquire(
            task.task_id,
            "runner-2",
            request,
            lease_seconds=30,
            now_ts=base + 2,
        )
        assert resource2 is not None
        assert resource2.generation == resource1.generation + 1

        with pytest.raises(LeaseLost):
            ops.execute(op)
        assert capability.calls == 0
        store.close()


def test_agent_waits_without_consuming_budget_when_resources_are_unavailable():
    with tempfile.TemporaryDirectory() as td:
        request = ResourceRequest(memory_mb=900)
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db", request=request
        )
        scheduler = TaskScheduler(tasks)
        task = tasks.create("heavy", ("done",), workspace_id="ws")
        scheduler.submit(task.task_id)
        lease = scheduler.claim_next("runner-1", lease_seconds=30)
        assert lease is not None

        refs = ReferentStack()
        refs.push("file_A", "selection")
        policy = PolicyEngine({
            "ws": WorkspacePolicy(allowed_capabilities=frozenset({"heavy"}))
        })
        runner = NativeAgentRunner(
            tasks,
            ops,
            Model(),
            policy,
            resources=ResourceBroker(
                ResourceCapacity(cpu_units=100, memory_mb=500, gpu_units=100),
                persistence=store,
            ),
            require_task_lease=True,
        )
        runner.step(task, build_manifest(task, world, refs), task_lease=lease)
        assert task.state is TaskState.WAITING
        assert task.wait_reason == "resource:unavailable"
        assert task.budget.operations_started == 0
        assert ops.operations == {}
        assert capability.calls == 0
        store.close()


def test_agent_records_fences_and_releases_resource_after_success():
    with tempfile.TemporaryDirectory() as td:
        request = ResourceRequest(cpu_units=20, memory_mb=200)
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db", request=request
        )
        scheduler = TaskScheduler(tasks)
        task = tasks.create("heavy", ("done",), workspace_id="ws")
        scheduler.submit(task.task_id)
        lease = scheduler.claim_next("runner-1", lease_seconds=30)
        assert lease is not None

        refs = ReferentStack()
        refs.push("file_A", "selection")
        policy = PolicyEngine({
            "ws": WorkspacePolicy(allowed_capabilities=frozenset({"heavy"}))
        })
        broker = ResourceBroker(
            ResourceCapacity(cpu_units=100, memory_mb=1000, gpu_units=100),
            persistence=store,
        )
        runner = NativeAgentRunner(
            tasks,
            ops,
            Model(),
            policy,
            resources=broker,
            require_task_lease=True,
        )
        runner.step(task, build_manifest(task, world, refs), task_lease=lease)
        assert capability.calls == 1
        op = next(iter(ops.operations.values()))
        assert op.task_lease_owner_id == lease.owner_id
        assert op.task_lease_generation == lease.generation
        assert op.resource_lease_generation == 1
        assert store.conn.execute(
            "SELECT COUNT(*) AS n FROM resource_leases"
        ).fetchone()["n"] == 0
        store.close()


def test_memory_event_processing_is_serialized_across_connections():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        setup = SQLiteStore(db)
        world = WorldModel(persistence=setup)
        events = EventStore(persistence=setup)
        events.enqueue_outbox(
            event_type="memory.candidate",
            actor="user",
            workspace_id="ws",
            payload={
                "kind": "FACT",
                "key": "theme",
                "value": "dark",
                "scope": "project:web",
                "confidence": 1.0,
                "explicit_user_statement": True,
            },
        )
        event = events.flush_outbox()[0]
        candidate = MemoryCandidate(
            workspace_id="ws",
            kind=MemoryKind.FACT,
            key="theme",
            value="dark",
            scope="project:web",
            source_event_ids=(event.event_id,),
            confidence=1.0,
            explicit_user_statement=True,
        )
        setup.close()

        ids: list[str | None] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def worker():
            store = SQLiteStore(db)
            try:
                barrier.wait(timeout=5)
                record = store.process_memory_event(event, candidate)
                ids.append(record.memory_id if record else None)
            except BaseException as exc:
                errors.append(exc)
            finally:
                store.close()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert errors == []
        assert len(ids) == 2
        assert ids[0] == ids[1]

        verify = SQLiteStore(db)
        rows = verify.conn.execute(
            "SELECT * FROM memory_records WHERE workspace_id='ws' AND key='theme'"
        ).fetchall()
        receipts = verify.conn.execute(
            "SELECT * FROM consumer_receipts WHERE consumer_id='memory' AND event_id=?",
            (event.event_id,),
        ).fetchall()
        assert len(rows) == 1
        assert len(receipts) == 1
        verify.close()


def test_persistent_agent_requires_task_lease_by_default():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db"
        )
        task = tasks.create("work", ("done",), workspace_id="ws")
        task.desired_state = TaskState.RUNNING
        refs = ReferentStack()
        refs.push("file_A", "selection")
        policy = PolicyEngine({
            "ws": WorkspacePolicy(allowed_capabilities=frozenset({"heavy"}))
        })
        runner = NativeAgentRunner(tasks, ops, Model(), policy)
        runner.step(task, build_manifest(task, world, refs))
        assert task.state is TaskState.WAITING
        assert task.wait_reason == "lease:required"
        assert ops.operations == {}
        assert capability.calls == 0
        store.close()


def test_released_task_lease_never_reuses_generation():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks, ops, capability = build_core(
            Path(td) / "native.db"
        )
        scheduler = TaskScheduler(tasks)
        task = tasks.create("work", ("done",), workspace_id="ws")
        scheduler.submit(task.task_id)
        base = now()
        lease1 = scheduler.claim_next("runner", lease_seconds=30, now_ts=base)
        assert lease1 is not None
        assert scheduler.release(lease1)
        lease2 = scheduler.claim_next("runner", lease_seconds=30, now_ts=base + 1)
        assert lease2 is not None
        assert lease2.generation == lease1.generation + 1
        assert not store.validate_task_lease_identity(
            lease1.task_id, lease1.owner_id, lease1.generation, now_ts=base + 1
        )
        store.close()


def test_released_resource_lease_never_reuses_generation_or_allows_active_steal():
    with tempfile.TemporaryDirectory() as td:
        store = SQLiteStore(Path(td) / "native.db")
        broker = ResourceBroker(
            ResourceCapacity(cpu_units=100, memory_mb=1000, gpu_units=100),
            persistence=store,
        )
        request = ResourceRequest(memory_mb=400)
        base = now()
        lease1 = broker.acquire(
            "task-1", "runner-1", request, lease_seconds=30, now_ts=base
        )
        assert lease1 is not None
        assert broker.acquire(
            "task-1", "runner-2", request, lease_seconds=30, now_ts=base + 1
        ) is None
        assert broker.release(lease1)
        lease2 = broker.acquire(
            "task-1", "runner-2", request, lease_seconds=30, now_ts=base + 2
        )
        assert lease2 is not None
        assert lease2.generation == lease1.generation + 1
        assert not store.validate_resource_lease_identity(
            lease1.task_id, lease1.owner_id, lease1.generation, now_ts=base + 2
        )
        store.close()
