from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .models import TaskState
from .runtime import TaskRuntime


class ControlIntent(str, Enum):
    STOP_SPEAKING = "stop_speaking"
    PAUSE_TASK = "pause_task"
    RESUME_TASK = "resume_task"
    CANCEL_TASK = "cancel_task"


@dataclass(frozen=True, slots=True)
class ControlResolution:
    intent: ControlIntent
    task_id: str | None
    needs_user: bool = False
    reason: str | None = None


class ControlIntentResolver:
    def resolve(self, intent: ControlIntent, tasks: TaskRuntime, explicit_task_id: str | None = None) -> ControlResolution:
        if intent is ControlIntent.STOP_SPEAKING:
            return ControlResolution(intent, None)
        if explicit_task_id:
            return ControlResolution(intent, explicit_task_id)
        if intent is ControlIntent.RESUME_TASK:
            candidates = [t.task_id for t in tasks.tasks.values() if t.state in {TaskState.PAUSED, TaskState.WAITING}]
        else:
            candidates = [t.task_id for t in tasks.tasks.values() if t.state in {TaskState.RUNNING, TaskState.QUEUED, TaskState.WAITING}]
        if len(candidates) == 1:
            return ControlResolution(intent, candidates[0])
        if not candidates:
            return ControlResolution(intent, None, True, "no_matching_task")
        return ControlResolution(intent, None, True, "ambiguous_task")

    def apply(self, resolution: ControlResolution, tasks: TaskRuntime):
        if resolution.needs_user or resolution.task_id is None:
            return None
        mapping = {
            ControlIntent.PAUSE_TASK: TaskState.PAUSED,
            ControlIntent.RESUME_TASK: TaskState.RUNNING,
            ControlIntent.CANCEL_TASK: TaskState.CANCELLED,
        }
        desired = mapping.get(resolution.intent)
        return tasks.control(resolution.task_id, desired) if desired else None
