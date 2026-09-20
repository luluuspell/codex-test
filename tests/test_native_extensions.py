from liandanlu_native.brain import BrainSnapshot, FlyConsumerState
from liandanlu_native.control import ControlIntent, ControlIntentResolver
from liandanlu_native.events import EventStore
from liandanlu_native.memory import MemoryCandidate, MemoryKind, MemoryStore, StrategyState
from liandanlu_native.models import RiskClass, TaskState
from liandanlu_native.policy import PolicyDecision, PolicyEngine, WorkspacePolicy
from liandanlu_native.runtime import TaskRuntime
from liandanlu_native.world import WorldModel


def make_tasks():
    world = WorldModel()
    events = EventStore()
    return TaskRuntime(world, events)


def test_control_intent_never_confuses_stop_speaking_with_task_cancel():
    tasks = make_tasks()
    task = tasks.create("long work", ("done",))
    task.state = TaskState.RUNNING
    resolver = ControlIntentResolver()
    speech = resolver.resolve(ControlIntent.STOP_SPEAKING, tasks)
    assert speech.task_id is None and not speech.needs_user
    cancel = resolver.resolve(ControlIntent.CANCEL_TASK, tasks)
    assert cancel.task_id == task.task_id


def test_control_resolver_requests_user_when_multiple_tasks_match():
    tasks = make_tasks()
    a = tasks.create("a", ("done",))
    b = tasks.create("b", ("done",))
    a.state = b.state = TaskState.RUNNING
    res = ControlIntentResolver().resolve(ControlIntent.PAUSE_TASK, tasks)
    assert res.needs_user and res.reason == "ambiguous_task"


def test_policy_is_default_deny_and_confirms_mutations():
    engine = PolicyEngine({
        "ws": WorkspacePolicy(allowed_capabilities=frozenset({"web"}))
    })
    assert engine.evaluate("missing", "web", "inspect", RiskClass.READ) is PolicyDecision.DENY
    assert engine.evaluate("ws", "web", "inspect", RiskClass.READ) is PolicyDecision.ALLOW
    assert engine.evaluate("ws", "web", "edit", RiskClass.MUTATING) is PolicyDecision.REQUIRE_CONFIRMATION
    assert engine.evaluate("ws", "web", "deploy", RiskClass.EXTERNAL) is PolicyDecision.REQUIRE_CONFIRMATION
    assert engine.evaluate("ws", "files", "read", RiskClass.READ) is PolicyDecision.DENY


def test_memory_refuses_unconfirmed_global_inference():
    store = MemoryStore()
    candidate = MemoryCandidate(
        MemoryKind.FACT, "dark_theme", True, "global", ("e1",), 0.8, False
    )
    assert store.commit(candidate) is None
    candidate.explicit_user_statement = True
    assert store.commit(candidate) is not None


def test_memory_revision_supersedes_without_destroying_old_fact():
    store = MemoryStore()
    r1 = store.commit(MemoryCandidate(
        MemoryKind.FACT, "price", 299, "project:store", ("e1",), 1.0, True
    ))
    r2 = store.commit(MemoryCandidate(
        MemoryKind.FACT, "price", 319, "project:store", ("e2",), 1.0, True
    ))
    assert r2.revision == 2 and r2.supersedes == r1.memory_id
    assert store.records[r1.memory_id].value == 299


def test_strategy_requires_accumulated_support_before_promotion():
    store = MemoryStore()
    record = store.commit(MemoryCandidate(
        MemoryKind.STRATEGY, "vite_deploy", "build_first",
        "project:web", ("e1",), 0.7
    ))
    assert record.strategy_state is StrategyState.CANDIDATE
    store.add_strategy_support(record.memory_id)
    assert record.strategy_state is StrategyState.SUPPORTED
    store.add_strategy_support(record.memory_id)
    assert record.strategy_state is StrategyState.PROMOTED


def test_brain_snapshot_is_ignored_when_lagging_or_expired():
    snap = BrainSnapshot("b1", "brain-v1", "enc-v1", 100, valid_until_seq=110)
    assert snap.usable(105, max_lag=8)
    assert not snap.usable(109, max_lag=8)
    assert not snap.usable(111, max_lag=20)


def test_fly_consumer_reports_lag_without_blocking_engine_state():
    fly = FlyConsumerState("brain-v1", state="CATCHING_UP")
    fly.receive(120)
    fly.processed(100)
    assert fly.lag(120) == 20 and fly.state == "LAGGING"
    fly.processed(120)
    assert fly.lag(120) == 0 and fly.state == "LIVE"
