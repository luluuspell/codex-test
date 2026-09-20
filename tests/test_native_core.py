import sys

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry, IdempotencyMode
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import (
    ActionProposal, Entity, GoalClaim, OperationState, RiskClass, TaskState, new_id,
)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.supervisor import RuntimeSupervisor, ServiceState
from liandanlu_native.world import WorldModel, StaleWorld


class ReadCapability:
    def __init__(self, fail=False):
        self.fail = fail

    def execute(self, operation, locators):
        if self.fail:
            raise TimeoutError("response lost")
        return {"locator": locators[0], "content": "hello"}

    def verify(self, operation, result):
        return [{"claim": "read_result", "status": "pass" if result.get("content") else "fail"}]

    def reconcile(self, operation, locators):
        return True, {"locator": locators[0], "content": "reconciled"}


class Model:
    def __init__(self, ref):
        self.ref = ref

    def next_action(self, manifest):
        return ActionProposal(new_id("proposal"), "files", "read", (self.ref,))


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


def make_core():
    world = WorldModel()
    world.register(Entity(
        "file_A", "file", "ws", "/workspace/a.txt",
        permissions=frozenset({"read", "write"}),
    ))
    events = EventStore()
    tasks = TaskRuntime(world, events)
    ops = OperationRuntime(world, events, registry())
    return world, events, tasks, ops


def test_context_referent_ambiguity_and_manifest_freezes_revisions():
    world, events, tasks, ops = make_core()
    refs = ReferentStack()
    refs.push("file_A", "selection", 0.95)
    task = tasks.create("read it", ("read_done",), workspace_id="ws")
    manifest = build_manifest(task, world.revisions, refs, focus=("file_A",))
    assert refs.resolve() == "file_A"
    assert manifest.workspace_id == "ws"
    assert manifest.world_revisions["workspace"] == world.revisions.workspace
    refs.push("file_B", "recent", 0.90)
    refs.push("file_A", "selection", 0.95)
    assert refs.resolve(ambiguity_gap=0.10) is None


def test_world_domain_revision_blocks_stale_operation():
    world, events, tasks, ops = make_core()
    task = tasks.create("read", ("read_done",), workspace_id="ws")
    proposal = ActionProposal("p", "files", "read", ("file_A",))
    expected = {"workspace": world.revisions.workspace}
    world.observe("desktop", "selection", {"ref": "file_A"}, "workspace")
    with pytest.raises(StaleWorld):
        ops.prepare(task, proposal, expected_revisions=expected)


def test_agent_uses_object_ref_policy_and_verifier():
    world, events, tasks, ops = make_core()
    ops.capabilities["files"] = ReadCapability()
    task = tasks.create("read", ("read_done",), workspace_id="ws")
    task.desired_state = TaskState.RUNNING
    refs = ReferentStack()
    refs.push("file_A", "selection")
    manifest = build_manifest(task, world.revisions, refs)
    policy = PolicyEngine({"ws": WorkspacePolicy(
        allowed_capabilities=frozenset({"files"})
    )})
    runner = NativeAgentRunner(tasks, ops, Model("file_A"), policy)
    runner.step(task, manifest)
    op = list(ops.operations.values())[0]
    assert op.state is OperationState.VERIFIED
    assert op.workspace_id == "ws"
    assert op.result["locator"] == "/workspace/a.txt"
    assert events.all_events()[-1].event_type == "task.operation_verified"


def test_unknown_operation_is_reconciled_after_failure():
    world, events, tasks, ops = make_core()
    failing = ReadCapability(fail=True)
    ops.capabilities["files"] = failing
    task = tasks.create("read", ("read_done",), workspace_id="ws")
    task.desired_state = TaskState.RUNNING
    task.state = TaskState.RUNNING
    proposal = ActionProposal("p", "files", "read", ("file_A",))
    op = ops.prepare(
        task, proposal, expected_revisions={"workspace": world.revisions.workspace}
    )
    ops.execute(op)
    assert op.state is OperationState.UNKNOWN
    failing.fail = False
    RecoveryCoordinator(tasks, ops).recover()
    assert op.state is OperationState.VERIFIED


def test_goal_completion_requires_evidence_refs():
    world, events, tasks, ops = make_core()
    task = tasks.create("make output", ("file_created", "source_preserved"))
    tasks.evaluate_goal(task.task_id, {
        "file_created": GoalClaim("file_created", True, ("ev1",)),
        "source_preserved": GoalClaim("source_preserved", True, ()),
    })
    assert task.state is TaskState.RUNNING
    tasks.evaluate_goal(task.task_id, {
        "file_created": GoalClaim("file_created", True, ("ev1",)),
        "source_preserved": GoalClaim("source_preserved", True, ("ev2",)),
    })
    assert task.state is TaskState.COMPLETED


def test_scheduler_prioritizes_lane_then_priority():
    world, events, tasks, ops = make_core()
    scheduler = TaskScheduler(tasks)
    bg = tasks.create("build", ("done",), lane="background", priority=100)
    low = tasks.create("pause music", ("done",), lane="interactive", priority=10)
    high = tasks.create("stop speech", ("done",), lane="interactive", priority=90)
    scheduler.submit(bg.task_id)
    scheduler.submit(low.task_id)
    scheduler.submit(high.task_id)
    assert scheduler.next_task() == high.task_id
    assert scheduler.next_task() == low.task_id
    assert scheduler.next_task() == bg.task_id


def test_supervisor_managed_process_and_generation_guard():
    supervisor = RuntimeSupervisor()
    supervisor.register(
        "worker",
        command=(sys.executable, "-c", "import time; time.sleep(30)"),
        shutdown_deadline_s=0.5,
    )
    record = supervisor.start("worker")
    assert record.actual_state is ServiceState.STARTING
    supervisor.mark_ready("worker", generation=record.generation)
    assert record.actual_state is ServiceState.RUNNING
    with pytest.raises(ValueError):
        supervisor.heartbeat("worker", generation=record.generation - 1)
    supervisor.stop("worker")
    assert record.actual_state is ServiceState.STOPPED


def test_event_consumers_have_independent_cursors_in_memory_mode():
    store = EventStore()
    store.enqueue_outbox(event_type="x", actor="engine")
    store.flush_outbox()
    assert len(store.read_after("memory")) == 1
    assert len(store.read_after("fly")) == 1
    store.ack("memory", 1)
    assert len(store.read_after("memory")) == 0
    assert len(store.read_after("fly")) == 1
