from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import ActionProposal, Entity, OperationState, RiskClass, TaskState, new_id
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.supervisor import RuntimeSupervisor, ServiceState
from liandanlu_native.world import WorldModel, StaleWorld


class ReadCapability:
    def __init__(self, fail=False):
        self.fail = fail
        self.values = {}
    def execute(self, operation, locators):
        if self.fail:
            raise TimeoutError("response lost")
        return {"locator": locators[0], "content": "hello"}
    def verify(self, operation, result):
        return [{"claim": "read_result", "status": "pass" if result.get("content") else "fail"}]
    def reconcile(self, operation, locators):
        return True, {"locator": locators[0], "content": "reconciled"}


class Model:
    def __init__(self, ref): self.ref = ref
    def next_action(self, manifest):
        return ActionProposal(new_id("proposal"), "files", "read", (self.ref,))


def make_core():
    world = WorldModel()
    file = Entity("file_A", "file", "ws", "/workspace/a.txt", permissions=frozenset({"read", "write"}))
    world.register(file)
    events = EventStore()
    tasks = TaskRuntime(world, events)
    ops = OperationRuntime(world, events)
    return world, events, tasks, ops


def test_context_referent_ambiguity_and_manifest_freezes_revisions():
    world, events, tasks, ops = make_core()
    refs = ReferentStack()
    refs.push("file_A", "selection", 0.95)
    task = tasks.create("read it", ("read_done",))
    manifest = build_manifest(task, world.revisions, refs, focus=("file_A",))
    assert refs.resolve() == "file_A"
    assert manifest.world_revisions["workspace"] == world.revisions.workspace
    refs.push("file_B", "recent", 0.90)
    refs.push("file_A", "selection", 0.95)
    assert refs.resolve(ambiguity_gap=0.10) is None


def test_world_domain_revision_blocks_stale_operation():
    world, events, tasks, ops = make_core()
    task = tasks.create("read", ("read_done",))
    proposal = ActionProposal("p", "files", "read", ("file_A",))
    expected = {"workspace": world.revisions.workspace}
    world.observe("desktop", "selection", {"ref": "file_A"}, "workspace")
    try:
        ops.prepare(task, proposal, risk=RiskClass.READ, expected_revisions=expected)
    except StaleWorld:
        pass
    else:
        raise AssertionError("stale world was not rejected")


def test_agent_uses_object_ref_and_verifier():
    world, events, tasks, ops = make_core()
    ops.capabilities["files"] = ReadCapability()
    task = tasks.create("read", ("read_done",))
    task.desired_state = TaskState.RUNNING
    refs = ReferentStack(); refs.push("file_A", "selection")
    manifest = build_manifest(task, world.revisions, refs)
    runner = NativeAgentRunner(tasks, ops, Model("file_A"))
    runner.step(task, manifest, expected_revisions={"workspace": world.revisions.workspace})
    op = list(ops.operations.values())[0]
    assert op.state is OperationState.VERIFIED
    assert op.result["locator"] == "/workspace/a.txt"
    assert events.events[-1].event_type == "operation.verified"


def test_unknown_operation_is_reconciled_after_crash():
    world, events, tasks, ops = make_core()
    ops.capabilities["files"] = ReadCapability(fail=True)
    task = tasks.create("read", ("read_done",)); task.desired_state = TaskState.RUNNING; task.state = TaskState.RUNNING
    proposal = ActionProposal("p", "files", "read", ("file_A",))
    op = ops.prepare(task, proposal, risk=RiskClass.READ, expected_revisions={"workspace": world.revisions.workspace})
    ops.execute(op)
    assert op.state is OperationState.UNKNOWN
    RecoveryCoordinator(tasks, ops).recover()
    assert op.state is OperationState.VERIFIED


def test_goal_completion_only_through_goal_evaluator():
    world, events, tasks, ops = make_core()
    task = tasks.create("make output", ("file_created", "source_preserved"))
    tasks.evaluate_goal(task.task_id, {"file_created": True, "source_preserved": False})
    assert task.state is TaskState.RUNNING
    tasks.evaluate_goal(task.task_id, {"file_created": True, "source_preserved": True})
    assert task.state is TaskState.COMPLETED


def test_scheduler_prioritizes_interactive_lane():
    world, events, tasks, ops = make_core()
    scheduler = TaskScheduler()
    bg = tasks.create("build", ("done",), lane="background")
    instant = tasks.create("pause music", ("done",), lane="interactive")
    scheduler.submit(bg); scheduler.submit(instant)
    assert scheduler.next_task() == instant.task_id
    assert scheduler.next_task() == bg.task_id


def test_supervisor_distinguishes_process_transport_functional_health():
    supervisor = RuntimeSupervisor(); supervisor.register("desktop_bridge")
    assert supervisor.assess("desktop_bridge", process_ok=True, transport_ok=False, functional_ok=False).actual_state is ServiceState.DEGRADED
    assert supervisor.assess("desktop_bridge", process_ok=True, transport_ok=True, functional_ok=True).actual_state is ServiceState.RUNNING
    rec = supervisor.restart("desktop_bridge")
    assert rec.generation == 2 and rec.actual_state is ServiceState.STARTING


def test_event_consumers_have_independent_cursors():
    store = EventStore()
    store.enqueue_outbox(event_type="x", actor="engine")
    store.flush_outbox()
    assert len(store.read_after("memory")) == 1
    assert len(store.read_after("fly")) == 1
    store.ack("memory", 1)
    assert len(store.read_after("memory")) == 0
    assert len(store.read_after("fly")) == 1
