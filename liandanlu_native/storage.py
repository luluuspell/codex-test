from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import (
    Entity, Event, Operation, OperationState, RiskClass, Task, TaskBudget,
    TaskLease, TaskPhase, TaskState, WorldRevisions, new_id, now,
)


class SQLiteStore:
    """Durable single-node store for Native Core."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        self.conn.close()

    def _columns(self, table: str) -> set[str]:
        return {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}

    def _ensure_column(self, table: str, name: str, ddl: str) -> None:
        if name not in self._columns(table):
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def _migrate(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks(
                task_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL DEFAULT 'default',
                goal TEXT NOT NULL,
                success_criteria_json TEXT NOT NULL,
                constraints_json TEXT NOT NULL,
                state TEXT NOT NULL,
                desired_state TEXT NOT NULL,
                phase TEXT NOT NULL,
                priority INTEGER NOT NULL,
                lane TEXT NOT NULL,
                queued_at REAL,
                wait_reason TEXT,
                budget_json TEXT NOT NULL DEFAULT '{}',
                revision INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operations(
                operation_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL DEFAULT 'default',
                capability TEXT NOT NULL,
                action TEXT NOT NULL,
                object_refs_json TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                risk_class TEXT NOT NULL,
                required_permission TEXT NOT NULL DEFAULT 'read',
                idempotency_mode TEXT NOT NULL DEFAULT 'RECONCILABLE',
                state TEXT NOT NULL,
                expected_revisions_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                result_json TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS world_entities(
                entity_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                locator TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                permissions_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS world_relations(
                source_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                PRIMARY KEY(source_id, relation_type, target_id)
            );

            CREATE TABLE IF NOT EXISTS world_revisions(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                global_revision INTEGER NOT NULL,
                desktop INTEGER NOT NULL,
                workspace INTEGER NOT NULL,
                browser INTEGER NOT NULL,
                media INTEGER NOT NULL,
                tasks INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspace_revisions(
                workspace_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_leases(
                task_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                claimed_at REAL NOT NULL,
                lease_until REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_outbox(
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                workspace_id TEXT NOT NULL DEFAULT 'system',
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT 'engine',
                task_id TEXT,
                operation_id TEXT,
                object_refs_json TEXT NOT NULL DEFAULT '[]',
                payload_json TEXT NOT NULL,
                causation_id TEXT,
                correlation_id TEXT,
                occurred_at REAL NOT NULL DEFAULT 0,
                learning_allowed INTEGER NOT NULL DEFAULT 1,
                dispatched INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                workspace_id TEXT NOT NULL DEFAULT 'system',
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                task_id TEXT,
                operation_id TEXT,
                object_refs_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                causation_id TEXT,
                correlation_id TEXT,
                occurred_at REAL NOT NULL,
                observed_at REAL NOT NULL,
                learning_allowed INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS consumer_cursors(
                consumer_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS consumer_receipts(
                consumer_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                status TEXT NOT NULL,
                result_ref TEXT,
                PRIMARY KEY(consumer_id, event_id)
            );

            CREATE TABLE IF NOT EXISTS memory_records(
                memory_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
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
                support_event_ids_json TEXT NOT NULL DEFAULT '[]',
                UNIQUE(workspace_id, scope, key, kind, revision)
            );
            """
        )
        self._ensure_column("tasks", "workspace_id", "TEXT NOT NULL DEFAULT 'default'")
        self._ensure_column("tasks", "queued_at", "REAL")
        self._ensure_column("tasks", "wait_reason", "TEXT")
        self._ensure_column("tasks", "budget_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_column("operations", "workspace_id", "TEXT NOT NULL DEFAULT 'default'")
        self._ensure_column("operations", "required_permission", "TEXT NOT NULL DEFAULT 'read'")
        self._ensure_column("operations", "idempotency_mode", "TEXT NOT NULL DEFAULT 'RECONCILABLE'")
        self._ensure_column("event_outbox", "workspace_id", "TEXT NOT NULL DEFAULT 'system'")
        self._ensure_column("events", "workspace_id", "TEXT NOT NULL DEFAULT 'system'")
        for name, ddl in (
            ("event_id", "TEXT"),
            ("actor", "TEXT NOT NULL DEFAULT 'engine'"),
            ("object_refs_json", "TEXT NOT NULL DEFAULT '[]'"),
            ("causation_id", "TEXT"),
            ("correlation_id", "TEXT"),
            ("occurred_at", "REAL NOT NULL DEFAULT 0"),
            ("learning_allowed", "INTEGER NOT NULL DEFAULT 1"),
        ):
            self._ensure_column("event_outbox", name, ddl)
        self._migrate_memory_schema()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO workspace_revisions(workspace_id, revision)
            SELECT DISTINCT workspace_id,
                   COALESCE((SELECT workspace FROM world_revisions WHERE singleton=1), 0)
            FROM world_entities
            """
        )
        self.conn.commit()

    def _migrate_memory_schema(self) -> None:
        columns = self._columns("memory_records")
        if "workspace_id" not in columns:
            self.conn.executescript(
                """
                ALTER TABLE memory_records RENAME TO memory_records_legacy;
                CREATE TABLE memory_records(
                    memory_id TEXT PRIMARY KEY,
                    workspace_id TEXT NOT NULL,
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
                    support_event_ids_json TEXT NOT NULL DEFAULT '[]',
                    UNIQUE(workspace_id, scope, key, kind, revision)
                );
                INSERT INTO memory_records(
                    memory_id,workspace_id,kind,key,value_json,scope,
                    source_event_ids_json,confidence,revision,supersedes,
                    strategy_state,support_count,support_event_ids_json
                )
                SELECT
                    memory_id,'legacy',kind,key,value_json,scope,
                    source_event_ids_json,confidence,revision,supersedes,
                    strategy_state,support_count,'[]'
                FROM memory_records_legacy;
                DROP TABLE memory_records_legacy;
                """
            )
        else:
            self._ensure_column(
                "memory_records", "support_event_ids_json",
                "TEXT NOT NULL DEFAULT '[]'"
            )
        self.conn.execute("DROP INDEX IF EXISTS idx_memory_latest")
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_memory_latest
            ON memory_records(workspace_id,scope,key,kind,revision DESC)
            """
        )

    def _insert_outbox(
        self, *, event_type: str, actor: str, workspace_id: str = "system", task_id: str | None = None,
        operation_id: str | None = None, object_refs: tuple[str, ...] = (),
        payload: dict[str, Any] | None = None, causation_id: str | None = None,
        correlation_id: str | None = None, learning_allowed: bool = True,
    ) -> str:
        event_id = new_id("evt")
        self.conn.execute(
            """
            INSERT INTO event_outbox(
                event_id,workspace_id,event_type,actor,task_id,operation_id,object_refs_json,
                payload_json,causation_id,correlation_id,occurred_at,learning_allowed,dispatched
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0)
            """,
            (
                event_id, workspace_id, event_type, actor, task_id, operation_id,
                json.dumps(object_refs), json.dumps(payload or {}),
                causation_id, correlation_id, now(), 1 if learning_allowed else 0,
            ),
        )
        return event_id

    def enqueue_event_outbox(self, **kwargs) -> str:
        with self.conn:
            return self._insert_outbox(**kwargs)

    def save_task(self, task: Task) -> None:
        with self.conn:
            self._upsert_task(task)

    def save_task_with_outbox_event(self, task: Task, **event_kwargs) -> str:
        with self.conn:
            self._upsert_task(task)
            return self._insert_outbox(task_id=task.task_id, **event_kwargs)

    def _upsert_task(self, task: Task) -> None:
        self.conn.execute(
            """
            INSERT INTO tasks(task_id, workspace_id, goal, success_criteria_json, constraints_json, state,
                              desired_state, phase, priority, lane, queued_at, wait_reason,
                              budget_json, revision)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(task_id) DO UPDATE SET
                workspace_id=excluded.workspace_id,
                goal=excluded.goal,
                success_criteria_json=excluded.success_criteria_json,
                constraints_json=excluded.constraints_json,
                state=excluded.state,
                desired_state=excluded.desired_state,
                phase=excluded.phase,
                priority=excluded.priority,
                lane=excluded.lane,
                queued_at=excluded.queued_at,
                wait_reason=excluded.wait_reason,
                budget_json=excluded.budget_json,
                revision=excluded.revision
            """,
            (
                task.task_id, task.workspace_id, task.goal, json.dumps(task.success_criteria),
                json.dumps(task.constraints), task.state.value,
                task.desired_state.value, task.phase.value, task.priority,
                task.lane, task.queued_at, task.wait_reason,
                json.dumps({
                    "max_operations": task.budget.max_operations,
                    "operations_started": task.budget.operations_started,
                    "deadline_at": task.budget.deadline_at,
                }),
                task.revision,
            ),
        )

    def load_tasks(self) -> dict[str, Task]:
        result: dict[str, Task] = {}
        for row in self.conn.execute("SELECT * FROM tasks"):
            budget_data = json.loads(row["budget_json"] or "{}")
            task = Task(
                task_id=row["task_id"], workspace_id=row["workspace_id"],
                goal=row["goal"],
                success_criteria=tuple(json.loads(row["success_criteria_json"])),
                constraints=tuple(json.loads(row["constraints_json"])),
                state=TaskState(row["state"]),
                desired_state=TaskState(row["desired_state"]),
                phase=TaskPhase(row["phase"]), priority=row["priority"],
                lane=row["lane"], queued_at=row["queued_at"],
                wait_reason=row["wait_reason"],
                budget=TaskBudget(
                    max_operations=int(budget_data.get("max_operations", 64)),
                    operations_started=int(budget_data.get("operations_started", 0)),
                    deadline_at=budget_data.get("deadline_at"),
                ),
                revision=row["revision"],
            )
            result[task.task_id] = task
        return result

    def save_operation(self, op: Operation) -> None:
        with self.conn:
            self._upsert_operation(op)

    def save_operation_with_outbox_event(self, op: Operation, **event_kwargs) -> str:
        with self.conn:
            self._upsert_operation(op)
            return self._insert_outbox(
                task_id=op.task_id, operation_id=op.operation_id,
                object_refs=op.object_refs, **event_kwargs
            )

    def save_task_and_operation_with_outbox_event(
        self, task: Task, op: Operation, **event_kwargs
    ) -> str:
        with self.conn:
            self._upsert_task(task)
            self._upsert_operation(op)
            return self._insert_outbox(
                task_id=task.task_id,
                operation_id=op.operation_id,
                object_refs=op.object_refs,
                **event_kwargs,
            )

    def _upsert_operation(self, op: Operation) -> None:
        self.conn.execute(
            """
            INSERT INTO operations(
                operation_id,task_id,workspace_id,capability,action,object_refs_json,arguments_json,
                risk_class,required_permission,idempotency_mode,state,
                expected_revisions_json,evidence_json,result_json,error
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(operation_id) DO UPDATE SET
                workspace_id=excluded.workspace_id,
                state=excluded.state,
                required_permission=excluded.required_permission,
                idempotency_mode=excluded.idempotency_mode,
                expected_revisions_json=excluded.expected_revisions_json,
                evidence_json=excluded.evidence_json,
                result_json=excluded.result_json,
                error=excluded.error
            """,
            (
                op.operation_id, op.task_id, op.workspace_id, op.capability, op.action,
                json.dumps(op.object_refs), json.dumps(op.arguments),
                op.risk_class.value, op.required_permission, op.idempotency_mode,
                op.state.value, json.dumps(op.expected_revisions),
                json.dumps(op.evidence),
                json.dumps(op.result) if op.result is not None else None,
                op.error,
            ),
        )

    def load_operations(self) -> dict[str, Operation]:
        result: dict[str, Operation] = {}
        for row in self.conn.execute("SELECT * FROM operations"):
            op = Operation(
                operation_id=row["operation_id"], task_id=row["task_id"],
                workspace_id=row["workspace_id"],
                capability=row["capability"], action=row["action"],
                object_refs=tuple(json.loads(row["object_refs_json"])),
                arguments=dict(json.loads(row["arguments_json"])),
                risk_class=RiskClass(row["risk_class"]),
                required_permission=row["required_permission"],
                idempotency_mode=row["idempotency_mode"],
                state=OperationState(row["state"]),
                expected_revisions=dict(json.loads(row["expected_revisions_json"])),
                evidence=list(json.loads(row["evidence_json"])),
                result=json.loads(row["result_json"]) if row["result_json"] else None,
                error=row["error"],
            )
            result[op.operation_id] = op
        return result

    def save_world_entity(self, entity: Entity) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO world_entities(
                    entity_id,entity_type,workspace_id,locator,version,status,
                    metadata_json,permissions_json
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(entity_id) DO UPDATE SET
                    entity_type=excluded.entity_type,
                    workspace_id=excluded.workspace_id,
                    locator=excluded.locator,
                    version=excluded.version,
                    status=excluded.status,
                    metadata_json=excluded.metadata_json,
                    permissions_json=excluded.permissions_json
                """,
                (
                    entity.entity_id, entity.entity_type, entity.workspace_id,
                    entity.locator, entity.version, entity.status,
                    json.dumps(entity.metadata), json.dumps(sorted(entity.permissions)),
                ),
            )

    def save_world_revisions(self, revisions: WorldRevisions) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO world_revisions(
                    singleton,global_revision,desktop,workspace,browser,media,tasks
                ) VALUES(1,?,?,?,?,?,?)
                ON CONFLICT(singleton) DO UPDATE SET
                    global_revision=excluded.global_revision,
                    desktop=excluded.desktop,
                    workspace=excluded.workspace,
                    browser=excluded.browser,
                    media=excluded.media,
                    tasks=excluded.tasks
                """,
                (
                    revisions.global_revision, revisions.desktop, revisions.workspace,
                    revisions.browser, revisions.media, revisions.tasks,
                ),
            )

    def save_workspace_revision(self, workspace_id: str, revision: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO workspace_revisions(workspace_id,revision) VALUES(?,?)
                ON CONFLICT(workspace_id) DO UPDATE SET revision=excluded.revision
                """,
                (workspace_id, revision),
            )

    def save_world_relation(self, source: str, relation: str, target: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO world_relations(source_id,relation_type,target_id) VALUES(?,?,?)",
                (source, relation, target),
            )

    def load_world_model(self):
        from .world import WorldModel
        row = self.conn.execute("SELECT * FROM world_revisions WHERE singleton=1").fetchone()
        revisions = WorldRevisions()
        if row:
            revisions = WorldRevisions(
                global_revision=row["global_revision"], desktop=row["desktop"],
                workspace=row["workspace"], browser=row["browser"],
                media=row["media"], tasks=row["tasks"],
            )
        workspace_revisions = {
            item["workspace_id"]: item["revision"]
            for item in self.conn.execute(
                "SELECT workspace_id,revision FROM workspace_revisions"
            )
        }
        world = WorldModel(
            revisions=revisions,
            workspace_revisions=workspace_revisions,
            persistence=self,
        )
        for item in self.conn.execute("SELECT * FROM world_entities"):
            entity = Entity(
                entity_id=item["entity_id"], entity_type=item["entity_type"],
                workspace_id=item["workspace_id"], locator=item["locator"],
                version=item["version"], status=item["status"],
                metadata=dict(json.loads(item["metadata_json"])),
                permissions=frozenset(json.loads(item["permissions_json"])),
            )
            world.entities[entity.entity_id] = entity
        for item in self.conn.execute("SELECT * FROM world_relations"):
            world.relations.setdefault(item["source_id"], []).append(
                (item["relation_type"], item["target_id"])
            )
        return world

    def dispatch_event_outbox(self, limit: int = 100) -> list[Event]:
        emitted: list[Event] = []
        with self.conn:
            rows = list(self.conn.execute(
                "SELECT * FROM event_outbox WHERE dispatched=0 ORDER BY outbox_id LIMIT ?",
                (limit,),
            ))
            for row in rows:
                event_id = row["event_id"] or new_id("evt")
                if row["event_id"] is None:
                    self.conn.execute(
                        "UPDATE event_outbox SET event_id=? WHERE outbox_id=?",
                        (event_id, row["outbox_id"]),
                    )
                observed_at = now()
                occurred_at = row["occurred_at"] or observed_at
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO events(
                        event_id,workspace_id,event_type,actor,task_id,operation_id,object_refs_json,
                        payload_json,causation_id,correlation_id,occurred_at,observed_at,
                        learning_allowed
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id, row["workspace_id"], row["event_type"], row["actor"], row["task_id"],
                        row["operation_id"], row["object_refs_json"], row["payload_json"],
                        row["causation_id"], row["correlation_id"], occurred_at,
                        observed_at, row["learning_allowed"],
                    ),
                )
                self.conn.execute(
                    "UPDATE event_outbox SET dispatched=1 WHERE outbox_id=?",
                    (row["outbox_id"],),
                )
                evrow = self.conn.execute(
                    "SELECT * FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
                emitted.append(self._row_to_event(evrow))
        return emitted

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        return Event(
            sequence=row["sequence"], event_id=row["event_id"],
            workspace_id=row["workspace_id"],
            event_type=row["event_type"], actor=row["actor"],
            task_id=row["task_id"], operation_id=row["operation_id"],
            object_refs=tuple(json.loads(row["object_refs_json"])),
            payload=dict(json.loads(row["payload_json"])),
            causation_id=row["causation_id"], correlation_id=row["correlation_id"],
            occurred_at=row["occurred_at"], observed_at=row["observed_at"],
            learning_allowed=bool(row["learning_allowed"]),
        )

    def read_events_after(self, consumer_id: str) -> list[Event]:
        cursor = self.conn.execute(
            "SELECT sequence FROM consumer_cursors WHERE consumer_id=?",
            (consumer_id,),
        ).fetchone()
        sequence = cursor["sequence"] if cursor else 0
        return [
            self._row_to_event(row)
            for row in self.conn.execute(
                "SELECT * FROM events WHERE sequence>? ORDER BY sequence", (sequence,)
            )
        ]

    def ack_event_consumer(self, consumer_id: str, sequence: int) -> None:
        current = self.conn.execute(
            "SELECT sequence FROM consumer_cursors WHERE consumer_id=?",
            (consumer_id,),
        ).fetchone()
        current_seq = current["sequence"] if current else 0
        if sequence < current_seq:
            return
        max_row = self.conn.execute("SELECT MAX(sequence) AS m FROM events").fetchone()
        max_seq = max_row["m"] or 0
        if sequence > max_seq:
            raise ValueError("cannot ack beyond durable event log")
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO consumer_cursors(consumer_id,sequence) VALUES(?,?)
                ON CONFLICT(consumer_id) DO UPDATE SET sequence=excluded.sequence
                """,
                (consumer_id, sequence),
            )


    def _row_to_memory(self, row: sqlite3.Row):
        from .memory import MemoryKind, MemoryRecord, StrategyState
        state = StrategyState(row["strategy_state"]) if row["strategy_state"] else None
        return MemoryRecord(
            memory_id=row["memory_id"],
            workspace_id=row["workspace_id"],
            kind=MemoryKind(row["kind"]),
            key=row["key"],
            value=json.loads(row["value_json"]),
            scope=row["scope"],
            source_event_ids=tuple(json.loads(row["source_event_ids_json"])),
            confidence=row["confidence"],
            revision=row["revision"],
            supersedes=row["supersedes"],
            strategy_state=state,
            support_count=row["support_count"],
            support_event_ids=tuple(json.loads(row["support_event_ids_json"])),
        )

    def load_memory_records(self) -> list:
        return [
            self._row_to_memory(row)
            for row in self.conn.execute(
                "SELECT * FROM memory_records ORDER BY scope,key,kind,revision"
            )
        ]

    def save_memory_record(self, record) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO memory_records(
                    memory_id,workspace_id,kind,key,value_json,scope,source_event_ids_json,
                    confidence,revision,supersedes,strategy_state,support_count,
                    support_event_ids_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(memory_id) DO UPDATE SET
                    value_json=excluded.value_json,
                    confidence=excluded.confidence,
                    strategy_state=excluded.strategy_state,
                    support_count=excluded.support_count,
                    support_event_ids_json=excluded.support_event_ids_json
                """,
                (
                    record.memory_id, record.workspace_id, record.kind.value, record.key,
                    json.dumps(record.value), record.scope,
                    json.dumps(record.source_event_ids), record.confidence,
                    record.revision, record.supersedes,
                    record.strategy_state.value if record.strategy_state else None,
                    record.support_count, json.dumps(record.support_event_ids),
                ),
            )

    def _advance_consumer_in_transaction(
        self,
        consumer_id: str,
        event_id: str,
        sequence: int,
        *,
        status: str,
        result_ref: str | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO consumer_receipts(
                consumer_id,event_id,sequence,status,result_ref
            ) VALUES(?,?,?,?,?)
            """,
            (consumer_id, event_id, sequence, status, result_ref),
        )
        self.conn.execute(
            """
            INSERT INTO consumer_cursors(consumer_id,sequence) VALUES(?,?)
            ON CONFLICT(consumer_id) DO UPDATE SET
                sequence=CASE
                    WHEN excluded.sequence > consumer_cursors.sequence
                    THEN excluded.sequence ELSE consumer_cursors.sequence
                END
            """,
            (consumer_id, sequence),
        )

    def process_memory_event(self, event: Event, candidate) -> Any:
        from .memory import MemoryKind, MemoryRecord, StrategyState

        consumer_id = "memory"
        with self.conn:
            receipt = self.conn.execute(
                """
                SELECT * FROM consumer_receipts
                WHERE consumer_id=? AND event_id=?
                """,
                (consumer_id, event.event_id),
            ).fetchone()
            if receipt is not None:
                if receipt["result_ref"]:
                    row = self.conn.execute(
                        "SELECT * FROM memory_records WHERE memory_id=?",
                        (receipt["result_ref"],),
                    ).fetchone()
                    return self._row_to_memory(row) if row else None
                return None

            if candidate is None:
                self._advance_consumer_in_transaction(
                    consumer_id, event.event_id, event.sequence,
                    status="ignored", result_ref=None,
                )
                return None

            previous = self.conn.execute(
                """
                SELECT * FROM memory_records
                WHERE workspace_id=? AND scope=? AND key=? AND kind=?
                ORDER BY revision DESC LIMIT 1
                """,
                (candidate.workspace_id, candidate.scope, candidate.key, candidate.kind.value),
            ).fetchone()
            revision = 1 if previous is None else previous["revision"] + 1
            state = (
                StrategyState.CANDIDATE
                if candidate.kind is MemoryKind.STRATEGY else None
            )
            record = MemoryRecord(
                memory_id=new_id("mem"), workspace_id=candidate.workspace_id,
                kind=candidate.kind, key=candidate.key,
                value=candidate.value, scope=candidate.scope,
                source_event_ids=candidate.source_event_ids,
                confidence=candidate.confidence, revision=revision,
                supersedes=previous["memory_id"] if previous else None,
                strategy_state=state,
            )
            self.conn.execute(
                """
                INSERT INTO memory_records(
                    memory_id,workspace_id,kind,key,value_json,scope,source_event_ids_json,
                    confidence,revision,supersedes,strategy_state,support_count,
                    support_event_ids_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record.memory_id, record.workspace_id, record.kind.value, record.key,
                    json.dumps(record.value), record.scope,
                    json.dumps(record.source_event_ids), record.confidence,
                    record.revision, record.supersedes,
                    record.strategy_state.value if record.strategy_state else None,
                    record.support_count, json.dumps(record.support_event_ids),
                ),
            )
            self._advance_consumer_in_transaction(
                consumer_id, event.event_id, event.sequence,
                status="committed", result_ref=record.memory_id,
            )
            return record

    def claim_next_task(
        self,
        owner_id: str,
        *,
        lease_seconds: float,
        now_ts: float | None = None,
    ) -> TaskLease | None:
        if not owner_id:
            raise ValueError("owner_id is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        ts = now() if now_ts is None else now_ts
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            row = self.conn.execute(
                """
                SELECT t.task_id, l.generation
                FROM tasks AS t
                LEFT JOIN task_leases AS l ON l.task_id=t.task_id
                WHERE t.state=?
                  AND t.desired_state=?
                  AND t.queued_at IS NOT NULL
                  AND (l.task_id IS NULL OR l.lease_until<=?)
                ORDER BY
                  CASE WHEN t.lane='interactive' THEN 0 ELSE 1 END,
                  t.priority DESC,
                  t.queued_at ASC,
                  t.task_id ASC
                LIMIT 1
                """,
                (TaskState.QUEUED.value, TaskState.RUNNING.value, ts),
            ).fetchone()
            if row is None:
                self.conn.commit()
                return None
            generation = 1 if row["generation"] is None else int(row["generation"]) + 1
            lease = TaskLease(
                task_id=row["task_id"],
                owner_id=owner_id,
                generation=generation,
                claimed_at=ts,
                lease_until=ts + lease_seconds,
            )
            self.conn.execute(
                """
                INSERT INTO task_leases(
                    task_id,owner_id,generation,claimed_at,lease_until
                ) VALUES(?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    owner_id=excluded.owner_id,
                    generation=excluded.generation,
                    claimed_at=excluded.claimed_at,
                    lease_until=excluded.lease_until
                """,
                (
                    lease.task_id, lease.owner_id, lease.generation,
                    lease.claimed_at, lease.lease_until,
                ),
            )
            self.conn.commit()
            return lease
        except Exception:
            self.conn.rollback()
            raise

    def renew_task_lease(
        self,
        lease: TaskLease,
        *,
        lease_seconds: float,
        now_ts: float | None = None,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        ts = now() if now_ts is None else now_ts
        renewed = TaskLease(
            task_id=lease.task_id,
            owner_id=lease.owner_id,
            generation=lease.generation,
            claimed_at=lease.claimed_at,
            lease_until=ts + lease_seconds,
        )
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE task_leases
                SET lease_until=?
                WHERE task_id=? AND owner_id=? AND generation=? AND lease_until>?
                """,
                (
                    renewed.lease_until, lease.task_id, lease.owner_id,
                    lease.generation, ts,
                ),
            )
        return renewed if cursor.rowcount == 1 else None

    def release_task_lease(self, lease: TaskLease) -> bool:
        with self.conn:
            cursor = self.conn.execute(
                """
                DELETE FROM task_leases
                WHERE task_id=? AND owner_id=? AND generation=?
                """,
                (lease.task_id, lease.owner_id, lease.generation),
            )
        return cursor.rowcount == 1

    def get_task_lease(self, task_id: str) -> TaskLease | None:
        row = self.conn.execute(
            "SELECT * FROM task_leases WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        return TaskLease(
            task_id=row["task_id"], owner_id=row["owner_id"],
            generation=row["generation"], claimed_at=row["claimed_at"],
            lease_until=row["lease_until"],
        )

    def pending_outbox(self) -> list[dict[str, Any]]:
        return [
            {
                "outbox_id": row["outbox_id"], "event_id": row["event_id"],
                "workspace_id": row["workspace_id"],
                "event_type": row["event_type"], "actor": row["actor"],
                "task_id": row["task_id"], "operation_id": row["operation_id"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in self.conn.execute(
                "SELECT * FROM event_outbox WHERE dispatched=0 ORDER BY outbox_id"
            )
        ]
