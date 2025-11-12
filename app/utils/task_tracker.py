"""
Training task tracker for async training operations
"""

from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum


class TaskStatus(str, Enum):
    """Training task status"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TrainingTask:
    """Represents a training task"""

    def __init__(self, task_id: str, request_data: Dict[str, Any]):
        self.task_id = task_id
        self.request_data = request_data
        self.status = TaskStatus.PENDING
        self.progress = 0.0
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self.result: Optional[Dict[str, Any]] = None

    def start(self):
        """Mark task as started"""
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.utcnow()

    def complete(self, result: Dict[str, Any]):
        """Mark task as completed"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.progress = 100.0
        self.result = result

    def fail(self, error: str):
        """Mark task as failed"""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error = error

    def update_progress(self, progress: float):
        """Update task progress"""
        self.progress = min(100.0, max(0.0, progress))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "message": self._get_message(),
            "progress": self.progress,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "result": self.result,
        }

    def _get_message(self) -> str:
        """Get status message"""
        if self.status == TaskStatus.PENDING:
            return "Training task queued"
        elif self.status == TaskStatus.RUNNING:
            return f"Training in progress ({self.progress:.1f}%)"
        elif self.status == TaskStatus.COMPLETED:
            return "Training completed successfully"
        elif self.status == TaskStatus.FAILED:
            return f"Training failed: {self.error}"
        return "Unknown status"


class TaskTracker:
    """Global task tracker for training jobs"""

    def __init__(self):
        self.tasks: Dict[str, TrainingTask] = {}

    def create_task(self, task_id: str, request_data: Dict[str, Any]) -> TrainingTask:
        """Create a new training task"""
        task = TrainingTask(task_id, request_data)
        self.tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> Optional[TrainingTask]:
        """Get a task by ID"""
        return self.tasks.get(task_id)

    def remove_task(self, task_id: str):
        """Remove a task"""
        self.tasks.pop(task_id, None)

    def list_tasks(self) -> Dict[str, TrainingTask]:
        """List all tasks"""
        return self.tasks


# Global tracker instance
task_tracker = TaskTracker()
