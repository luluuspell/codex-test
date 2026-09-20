import sqlite3
import tempfile
from pathlib import Path

from liandanlu_native.capabilities import CapabilityRegistry
from liandanlu_native.events import EventStore
from liandanlu_native.memory import MemoryKind, MemoryPipeline, MemoryStore, StrategyState
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


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
            workspace_id="store",
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
        record = memory.latest(
            "store", "project:store", "auto_publish", MemoryKind.FACT
        )
        assert record is not None and record.value is False
        assert record.workspace_id == "store"
        memory_id = record.memory_id
        store.close()

        store2 = SQLiteStore(db)
        events2 = EventStore(persistence=store2)
        memory2 = MemoryStore.from_persistence(store2)
        restored = memory2.latest(
            "store", "project:store", "auto_publish", MemoryKind.FACT
        )
        assert restored is not None and restored.memory_id == memory_id
        assert MemoryPipeline(events2, memory2).run_once()["scanned"] == 0
        store2.close()


def test_memory_event_commit_is_idempotent_if_same_event_is_replayed():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")
        events.enqueue_outbox(
            workspace_id="web",
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


def test_global_memory_requires_direct_user_event_and_is_partitioned_global():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")

        events.enqueue_outbox(
            workspace_id="ws-a",
            event_type="memory.candidate",
            actor="agent",
            payload={
                "kind": "FACT",
                "key": "always_dark",
                "value": True,
                "scope": "global",
                "confidence": 0.8,
                "explicit_user_statement": True,
            },
        )
        events.flush_outbox()
        memory = MemoryStore.from_persistence(store)
        result = MemoryPipeline(events, memory).run_once()
        assert result == {"scanned": 1, "committed": 0, "ignored": 1}
        assert store.load_memory_records() == []

        events.enqueue_outbox(
            workspace_id="ws-b",
            event_type="memory.candidate",
            actor="user",
            payload={
                "kind": "FACT",
                "key": "always_dark",
                "value": False,
                "scope": "global",
                "confidence": 1.0,
                "explicit_user_statement": True,
            },
        )
        events.flush_outbox()
        result2 = MemoryPipeline(events, memory).run_once()
        assert result2["committed"] == 1
        global_record = memory.latest(
            "any-workspace", "global", "always_dark", MemoryKind.FACT
        )
        assert global_record is not None
        assert global_record.workspace_id == "*"
        assert global_record.value is False
        store.close()


def test_same_memory_key_is_isolated_by_workspace():
    with tempfile.TemporaryDirectory() as td:
        store, world, events, tasks = make_durable(Path(td) / "native.db")
        for workspace_id, value in (("site-a", "red"), ("site-b", "blue")):
            events.enqueue_outbox(
                workspace_id=workspace_id,
                event_type="memory.candidate",
                actor="user",
                payload={
                    "kind": "FACT",
                    "key": "accent",
                    "value": value,
                    "scope": "project:site",
                    "confidence": 1.0,
                    "explicit_user_statement": True,
                },
            )
        events.flush_outbox()
        memory = MemoryStore.from_persistence(store)
        result = MemoryPipeline(events, memory).run_once()
        assert result["committed"] == 2
        a = memory.latest("site-a", "project:site", "accent", MemoryKind.FACT)
        b = memory.latest("site-b", "project:site", "accent", MemoryKind.FACT)
        assert a is not None and b is not None
        assert a.value == "red" and b.value == "blue"
        assert a.revision == b.revision == 1
        assert a.supersedes is None and b.supersedes is None
        store.close()


def test_strategy_support_requires_distinct_evidence_and_is_durable():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store, world, events, tasks = make_durable(db)
        events.enqueue_outbox(
            workspace_id="web",
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
        record = memory.latest(
            "web", "project:web", "vite_deploy", MemoryKind.STRATEGY
        )
        assert record is not None
        memory.add_strategy_support(record.memory_id, evidence_event_id="support-e2")
        count_after_first = record.support_count
        memory.add_strategy_support(record.memory_id, evidence_event_id="support-e2")
        assert record.support_count == count_after_first
        assert record.strategy_state is StrategyState.SUPPORTED
        memory.add_strategy_support(record.memory_id, evidence_event_id="support-e3")
        assert record.strategy_state is StrategyState.PROMOTED
        store.close()

        store2 = SQLiteStore(db)
        memory2 = MemoryStore.from_persistence(store2)
        restored = memory2.latest(
            "web", "project:web", "vite_deploy", MemoryKind.STRATEGY
        )
        assert restored is not None
        assert restored.strategy_state is StrategyState.PROMOTED
        assert restored.support_count == 3
        assert restored.support_event_ids == ("support-e2", "support-e3")
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


def test_a4_memory_schema_migrates_to_scoped_a5_schema():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE memory_records(
                memory_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                key TEXT NOT NULL,
                value_json TEXT NOT NULL,
                scope TEXT NOT NULL,
                source_event_ids_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                revision INTEGER NOT NULL,
                supersedes TEXT,
                strategy_state TEXT,
                support_count INTEGER NOT NULL,
                UNIQUE(scope, key, kind, revision)
            );
            INSERT INTO memory_records(
                memory_id,kind,key,value_json,scope,source_event_ids_json,
                confidence,revision,supersedes,strategy_state,support_count
            ) VALUES(
                'mem-old','FACT','theme','"dark"','project:legacy','["e1"]',
                1.0,1,NULL,NULL,1
            );
            """
        )
        conn.commit()
        conn.close()

        store = SQLiteStore(db)
        records = store.load_memory_records()
        assert len(records) == 1
        assert records[0].memory_id == "mem-old"
        assert records[0].workspace_id == "legacy"
        assert records[0].support_event_ids == ()
        store.close()
