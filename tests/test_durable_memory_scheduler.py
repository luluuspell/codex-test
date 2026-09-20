import tempfile
from pathlib import Path

from liandanlu_native.events import EventStore
from liandanlu_native.memory import MemoryKind, MemoryPipeline, MemoryStore, StrategyState
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel
from liandanlu_native.capabilities import CapabilityRegistry


def make_durable(path):
    store = SQLiteStore(path)
    world = WorldModel(persistence=store)
    events = EventStore(persistence=store)
    tasks = TaskRuntime(world, events, persistence=store)
    return store, world, events, tasks


def test_memory_pipeline_persists_fact_and_cursor_across_restart():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store, world, events, tasks = make_durable(db)
        events.enqueue_outbox(
            event_type="memory.candidate",
            actor="user",
            payload={
                "kind": "FACT",
                "key": "auto_publish",
                "value": False,
                "scope": "project:store",
                "confidence": 1.0,
                "explicit_user_statement": True,
            },
        )
        events.flush_outbox()
        memory = MemoryStore.from_persistence(store)
        result = MemoryPipeline(events, memory).run_once()
        assert result == {"scanned": 1, "committed": 1, "ignored": 0}
        record = memory.latest("project:store", "auto_publish", MemoryKind.FACT)
        assert record is not None and record.value is False
        memory_id = record.memory_id
        store.close()

        store2 = SQLiteStore(db)
        events2 = EventStore(persistence=store2)
        memory2 = MemoryStore.from_persistence(store2)
        restored = memory2.latest("project:store", "auto_publish", MemoryKind.FACT)
        assert restored is not None and restored.memory_id == memory_id
        assert MemoryPipeline(events2, memory2).run_once()["scanned"] == 0
        store2.close()


def test_memory_event_commit_is_idempotent_if_same_event_is_replayed():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")
        events.enqueue_outbox(
            event_type="memory.candidate",
            actor="user",
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
        memory = MemoryStore.from_persistence(store)
        candidate = MemoryPipeline(events, memory)._candidate_from_event(event)
        first = store.process_memory_event(event, candidate)
        second = store.process_memory_event(event, candidate)
        assert first is not None and second is not None
        assert first.memory_id == second.memory_id
        records = store.load_memory_records()
        assert [r.memory_id for r in records].count(first.memory_id) == 1
        store.close()


def test_global_memory_inference_is_rejected_but_cursor_advances():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")
        events.enqueue_outbox(
            event_type="memory.candidate",
            actor="agent",
            payload={
                "kind": "FACT",
                "key": "always_dark",
                "value": True,
                "scope": "global",
                "confidence": 0.8,
                "explicit_user_statement": False,
            },
        )
        events.flush_outbox()
        memory = MemoryStore.from_persistence(store)
        result = MemoryPipeline(events, memory).run_once()
        assert result == {"scanned": 1, "committed": 0, "ignored": 1}
        assert store.load_memory_records() == []
        assert events.read_after("memory") == []
        store.close()


def test_strategy_support_updates_are_durable():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")
        events.enqueue_outbox(
            event_type="memory.candidate",
            actor="engine",
            payload={
                "kind": "STRATEGY",
                "key": "vite_deploy",
                "value": "build_first",
                "scope": "project:web",
                "confidence": 0.7,
            },
        )
        events.flush_outbox()
        memory = MemoryStore.from_persistence(store)
        MemoryPipeline(events, memory).run_once()
        record = memory.latest("project:web", "vite_deploy", MemoryKind.STRATEGY)
        assert record is not None
        memory.add_strategy_support(record.memory_id)
        memory.add_strategy_support(record.memory_id)
        assert record.strategy_state is StrategyState.PROMOTED
        store.close()

        store2 = SQLiteStore(Path(td) / "native.db")
        memory2 = MemoryStore.from_persistence(store2)
        restored = memory2.latest("project:web", "vite_deploy", MemoryKind.STRATEGY)
        assert restored is not None
        assert restored.strategy_state is StrategyState.PROMOTED
        assert restored.support_count == 3
        store2.close()


def test_scheduler_rebuilds_from_durable_task_truth_after_restart():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store, world, events, tasks = make_durable(db)
        scheduler = TaskScheduler(tasks)
        background = tasks.create(
            "build site", ("done",), workspace_id="web",
            lane="background", priority=100,
        )
        instant = tasks.create(
            "pause music", ("done",), workspace_id="desktop",
            lane="interactive", priority=10,
        )
        scheduler.submit(background.task_id)
        scheduler.submit(instant.task_id)
        assert instant.queued_at is not None and background.queued_at is not None
        store.close()

        store2 = SQLiteStore(db)
        coordinator = RecoveryCoordinator.from_store(
            store2, registry=CapabilityRegistry(), capabilities={}
        )
        rebuilt = coordinator.rebuild_scheduler()
        assert rebuilt.next_task() == instant.task_id
        assert rebuilt.next_task() == background.task_id
        store2.close()
