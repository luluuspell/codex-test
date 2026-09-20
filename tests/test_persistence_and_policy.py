import tempfile
from pathlib import Path

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import ActionProposal, Entity, OperationState, RiskClass, TaskState, new_id
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
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
    def next_action(self, manifest):
        return ActionProposal(new_id("proposal"), "files", "read", ("file_A",))


def build_persistent_core(path):
    world = WorldModel()
    world.register(Entity("file_A", "file", "ws", "/workspace/a.txt", permissions=frozenset({"read", "write"})))
    events = EventStore()
    store = SQLiteStore(path)
    tasks = TaskRuntime(world, events, persistence=store)
    ops = OperationRuntime(world, events, persistence=store)
    return world, events, store, tasks, ops


def test_sqlite_restart_restores_task_and_unknown_operation_without_repeating_effect():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        world, events, store, tasks, ops = build_persistent_core(db)
        ops.capabilities["files"] = Capability(fail=True)
        task = tasks.create("read", ("read_done",))
        task.desired_state = TaskState.RUNNING
        proposal = ActionProposal("p", "files", "read", ("file_A",))
        op = ops.prepare(task, proposal, risk=RiskClass.READ, expected_revisions={"workspace": world.revisions.workspace})
        ops.execute(op)
        assert op.state is OperationState.UNKNOWN
        assert any(x["event_type"] == "operation.unknown" for x in store.pending_outbox())
        store.close()

        store2 = SQLiteStore(db)
        loaded_tasks = store2.load_tasks()
        loaded_ops = store2.load_operations()
        assert loaded_tasks[task.task_id].goal == "read"
        assert loaded_ops[op.operation_id].state is OperationState.UNKNOWN
        assert len(store2.pending_outbox()) >= 2
        store2.close()


def test_agent_policy_blocks_or_waits_before_operation_creation():
    with tempfile.TemporaryDirectory() as td:
        world, events, store, tasks, ops = build_persistent_core(Path(td) / "native.db")
        ops.capabilities["files"] = Capability()
        task = tasks.create("read", ("done",))
        task.desired_state = TaskState.RUNNING
        refs = ReferentStack(); refs.push("file_A", "selection")
        manifest = build_manifest(task, world.revisions, refs)

        deny_policy = PolicyEngine({"ws": WorkspacePolicy(allowed_capabilities=frozenset({"web"}))})
        runner = NativeAgentRunner(tasks, ops, Model(), policy=deny_policy, workspace_id="ws")
        runner.step(task, manifest, expected_revisions={"workspace": world.revisions.workspace})
        assert task.state is TaskState.BLOCKED
        assert ops.operations == {}

        task2 = tasks.create("read external", ("done",))
        task2.desired_state = TaskState.RUNNING
        allow_files = WorkspacePolicy(allowed_capabilities=frozenset({"files"}))
        runner2 = NativeAgentRunner(tasks, ops, Model(), policy=PolicyEngine({"ws": allow_files}), workspace_id="ws")
        runner2.step(task2, build_manifest(task2, world.revisions, refs), risk=RiskClass.EXTERNAL, expected_revisions={"workspace": world.revisions.workspace})
        assert task2.state is TaskState.WAITING
        store.close()
