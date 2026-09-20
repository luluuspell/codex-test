from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import Operation, OperationState, RiskClass, Task, TaskPhase, TaskState


class SQLiteStore:
    """Durable local store for the Native Core reference implementation.

    This is intentionally small and explicit. It persists authoritative task/operation
    state and a transactional outbox so recovery can reconcile UNKNOWN work without
    repeating side effects.
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        self.conn.close()

    def _migrate(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks(
                task_id TEXT PRIMARY KEY,
                goal TEXT NOT NULL,
                success_criteria_json TEXT NOT NULL,
                constraints_json TEXT NOT NULL,
                state TEXT NOT NULL,
                desired_state TEXT NOT NULL,
                phase TEXT NOT NULL,
                priority INTEGER NOT NULL,
                lane TEXT NOT NULL,
                revision INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operations(
                operation_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                capability TEXT NOT NULL,
                action TEXT NOT NULL,
                object_refs_json TEXT NOT NULL,
                arguments_json TEXT NOT NULL,
                risk_class TEXT NOT NULL,
                state TEXT NOT NULL,
                expected_revisions_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                result_json TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS event_outbox(
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                task_id TEXT,
                operation_id TEXT,
                payload_json TEXT NOT NULL,
                dispatched INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self.conn.commit()

    def save_task(self, task: Task) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO tasks(task_id, goal, success_criteria_json, constraints_json, state,
                                  desired_state, phase, priority, lane, revision)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    goal=excluded.goal,
                    success_criteria_json=excluded.success_criteria_json,
                    constraints_json=excluded.constraints_json,
                    state=excluded.state,
                    desired_state=excluded.desired_state,
                    phase=excluded.phase,
                    priority=excluded.priority,
                    lane=excluded.lane,
                    revision=excluded.revision
                """,
                (
                    task.task_id, task.goal, json.dumps(task.success_criteria), json.dumps(task.constraints),
                    task.state.value, task.desired_state.value, task.phase.value, task.priority, task.lane,
                    task.revision,
                ),
            )

    def load_tasks(self) -> dict[str, Task]:
        result: dict[str, Task] = {}
        for row in self.conn.execute("SELECT * FROM tasks"):
            task = Task(
                task_id=row["task_id"],
                goal=row["goal"],
                success_criteria=tuple(json.loads(row["success_criteria_json"])),
                constraints=tuple(json.loads(row["constraints_json"])),
                state=TaskState(row["state"]),
                desired_state=TaskState(row["desired_state"]),
                phase=TaskPhase(row["phase"]),
                priority=row["priority"],
                lane=row["lane"],
                revision=row["revision"],
            )
            result[task.task_id] = task
        return result

    def save_operation(self, op: Operation) -> None:
        with self.conn:
            self._upsert_operation(op)

    def save_operation_with_outbox(self, op: Operation, *, event_type: str, payload: dict[str, Any] | None = None) -> None:
        """Atomically persist operation state and its durable event intent."""
        with self.conn:
            self._upsert_operation(op)
            self.conn.execute(
                """
                INSERT INTO event_outbox(event_type, task_id, operation_id, payload_json, dispatched)
                VALUES(?,?,?,?,0)
                """,
                (event_type, op.task_id, op.operation_id, json.dumps(payload or {})),
            )

    def _upsert_operation(self, op: Operation) -> None:
        self.conn.execute(
            """
            INSERT INTO operations(operation_id, task_id, capability, action, object_refs_json,
                                   arguments_json, risk_class, state, expected_revisions_json,
                                   evidence_json, result_json, error)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(operation_id) DO UPDATE SET
                state=excluded.state,
                expected_revisions_json=excluded.expected_revisions_json,
                evidence_json=excluded.evidence_json,
                result_json=excluded.result_json,
                error=excluded.error
            """,
            (
                op.operation_id, op.task_id, op.capability, op.action, json.dumps(op.object_refs),
                json.dumps(op.arguments), op.risk_class.value, op.state.value,
                json.dumps(op.expected_revisions), json.dumps(op.evidence),
                json.dumps(op.result) if op.result is not None else None, op.error,
            ),
        )

    def load_operations(self) -> dict[str, Operation]:
        result: dict[str, Operation] = {}
        for row in self.conn.execute("SELECT * FROM operations"):
            op = Operation(
                operation_id=row["operation_id"],
                task_id=row["task_id"],
                capability=row["capability"],
                action=row["action"],
                object_refs=tuple(json.loads(row["object_refs_json"])),
                arguments=dict(json.loads(row["arguments_json"])),
                risk_class=RiskClass(row["risk_class"]),
                state=OperationState(row["state"]),
                expected_revisions=dict(json.loads(row["expected_revisions_json"])),
                evidence=list(json.loads(row["evidence_json"])),
                result=json.loads(row["result_json"]) if row["result_json"] else None,
                error=row["error"],
            )
            result[op.operation_id] = op
        return result

    def pending_outbox(self) -> list[dict[str, Any]]:
        return [
            {
                "outbox_id": row["outbox_id"],
                "event_type": row["event_type"],
                "task_id": row["task_id"],
                "operation_id": row["operation_id"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in self.conn.execute(
                "SELECT * FROM event_outbox WHERE dispatched=0 ORDER BY outbox_id"
            )
        ]

    def mark_outbox_dispatched(self, outbox_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE event_outbox SET dispatched=1 WHERE outbox_id=?",
                (outbox_id,),
            )
