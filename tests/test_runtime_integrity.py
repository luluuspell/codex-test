import tempfile
from pathlib import Path

from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry
from liandanlu_native.events import EventStore
from liandanlu_native.models import Entity, GoalClaim, RiskClass, TaskState
from liandanlu_native.runtime import TaskRuntime
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


def test_world_identity_relations_and_domain_revisions_survive_restart():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "native.db"
        store = SQLiteStore(db)
        world = WorldModel(persistence=store)
        world.register(Entity("product_1", "product", "ws", "provider:1"))
        world.register(Entity("page_1", "page", "ws", "/pages/1"))
        world.relate("product_1", "landing_page", "page_1")
        expected_revision = world.revisions.workspace
        store.close()

        store2 = SQLiteStore(db)
        restored = store2.load_world_model()
        assert restored.get("product_1").locator == "provider:1"
        assert restored.relations["product_1"] == [("landing_page", "page_1")]
        assert restored.revisions.workspace == expected_revision
        store2.close()


def test_persistent_event_store_uses_single_durable_outbox():
    with tempfile.TemporaryDirectory() as td:
        store = SQLiteStore(Path(td) / "native.db")
        events = EventStore(persistence=store)
        events.enqueue_outbox(
            event_type="x", actor="engine", payload={"a": 1}
        )
        assert events._outbox == []
        assert len(store.pending_outbox()) == 1
        first = events.flush_outbox()
        second = events.flush_outbox()
        assert len(first) == 1 and second == []
        assert len(events.read_after("audit")) == 1
        store.close()


def test_goal_claim_boolean_without_evidence_cannot_complete_task():
    world = WorldModel()
    events = EventStore()
    tasks = TaskRuntime(world, events)
    task = tasks.create("publish preview", ("preview_reachable",))
    tasks.evaluate_goal(task.task_id, {
        "preview_reachable": GoalClaim("preview_reachable", True, ())
    })
    assert task.state is TaskState.RUNNING
    tasks.evaluate_goal(task.task_id, {
        "preview_reachable": GoalClaim(
            "preview_reachable", True, ("http_probe_200",)
        )
    })
    assert task.state is TaskState.COMPLETED


def test_capability_registry_is_the_risk_authority():
    registry = CapabilityRegistry()
    registry.register(ActionSpec(
        "web", "deploy", RiskClass.EXTERNAL, "publish"
    ))
    spec = registry.actions[("web", "deploy")]
    assert spec.risk_class is RiskClass.EXTERNAL
    assert spec.required_permission == "publish"
