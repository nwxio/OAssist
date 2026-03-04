import threading
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.sync import run_full_sync


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class SyncJobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state: dict[str, Any] = {
            "job_id": None,
            "status": "idle",
            "started_at": None,
            "finished_at": None,
            "message": None,
            "total_documents": 0,
            "processed_documents": 0,
            "indexed_documents": 0,
            "indexed_chunks": 0,
            "failed_documents": 0,
            "progress_percent": 0.0,
            "duration_seconds": None,
        }

    def is_running(self) -> bool:
        with self._lock:
            return self._state["status"] == "running"

    def status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def start(self, settings: Settings) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] == "running":
                raise RuntimeError("sync job already running")

            job_id = str(uuid.uuid4())
            self._state = {
                "job_id": job_id,
                "status": "running",
                "started_at": _utc_now(),
                "finished_at": None,
                "message": None,
                "total_documents": 0,
                "processed_documents": 0,
                "indexed_documents": 0,
                "indexed_chunks": 0,
                "failed_documents": 0,
                "progress_percent": 0.0,
                "duration_seconds": None,
            }

            self._thread = threading.Thread(
                target=self._run_job,
                args=(job_id, settings),
                daemon=True,
                name=f"sync-job-{job_id}",
            )
            self._thread.start()
            return dict(self._state)

    def _update_progress(self, job_id: str, progress: dict[str, int | float]) -> None:
        with self._lock:
            if self._state.get("job_id") != job_id or self._state.get("status") != "running":
                return
            self._state.update(progress)

    def _run_job(self, job_id: str, settings: Settings) -> None:
        def progress_callback(progress: dict[str, int | float]) -> None:
            self._update_progress(job_id, progress)

        try:
            result = run_full_sync(settings, progress_callback=progress_callback)
            with self._lock:
                if self._state.get("job_id") == job_id:
                    self._state.update(result)
                    self._state["status"] = "completed"
                    self._state["finished_at"] = _utc_now()
                    self._state["message"] = "sync completed"
        except Exception as exc:
            with self._lock:
                if self._state.get("job_id") == job_id:
                    self._state["status"] = "failed"
                    self._state["finished_at"] = _utc_now()
                    self._state["message"] = str(exc)
        finally:
            with self._lock:
                self._thread = None


sync_jobs = SyncJobManager()
