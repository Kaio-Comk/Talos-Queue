"""Thin synchronous wrapper over the queue's RPCs.

Calls are SYNCHRONOUS on purpose (supabase-py is sync). From async code run them
with `asyncio.to_thread`; `run_worker` already does.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Job:
    id: int
    kind: str
    dedupe_key: str
    payload: dict = field(default_factory=dict)
    status: str = "queued"
    attempts: int = 0

    @classmethod
    def from_row(cls, row: Any) -> "Job":
        if not isinstance(row, dict) or row.get("id") is None:
            raise ValueError(f"invalid job row: {row!r}")
        return cls(
            id=int(row["id"]),
            kind=str(row["kind"]),
            dedupe_key=str(row["dedupe_key"]),
            payload=row.get("payload") or {},
            status=str(row.get("status") or "queued"),
            attempts=int(row.get("attempts") or 0),
        )


class LeaseQueue:
    def __init__(self, supabase):
        self.sb = supabase

    def enqueue(self, kind: str, dedupe_key: str, payload: Optional[dict] = None,
                priority: int = 100) -> Job:
        """Returns the existing active job when (kind, dedupe_key) is already queued/running."""
        res = self.sb.rpc("enqueue_job", {
            "p_kind": kind, "p_dedupe_key": dedupe_key,
            "p_payload": payload or {}, "p_priority": priority,
        }).execute()
        return Job.from_row(res.data)

    def claim(self, worker_id: str, kind: Optional[str] = None,
              lease_seconds: int = 300) -> Optional[Job]:
        res = self.sb.rpc("claim_job", {
            "p_worker_id": worker_id, "p_kind": kind, "p_lease_seconds": lease_seconds,
        }).execute()
        data = res.data
        # An empty claim comes back as a composite with every field NULL, not a bare null.
        if not data or data.get("id") is None:
            return None
        return Job.from_row(data)

    def heartbeat(self, job_id: int, worker_id: str, lease_seconds: int = 300) -> bool:
        res = self.sb.rpc("heartbeat_job", {
            "p_job_id": job_id, "p_worker_id": worker_id, "p_lease_seconds": lease_seconds,
        }).execute()
        return bool(res.data)

    def set_stage(self, job_id: int, worker_id: str, stage: str,
                  lease_seconds: int = 300) -> bool:
        res = self.sb.rpc("set_job_stage", {
            "p_job_id": job_id, "p_worker_id": worker_id, "p_stage": stage,
            "p_lease_seconds": lease_seconds,
        }).execute()
        return bool(res.data)

    def finish(self, job_id: int, worker_id: str, result: Optional[dict] = None) -> bool:
        res = self.sb.rpc("finish_job", {
            "p_job_id": job_id, "p_worker_id": worker_id, "p_result": result,
        }).execute()
        return bool(res.data)

    def fail(self, job_id: int, worker_id: str, error: str, *, retry: bool = False,
             retry_seconds: int = 60, max_attempts: int = 5) -> bool:
        res = self.sb.rpc("fail_job", {
            "p_job_id": job_id, "p_worker_id": worker_id, "p_error": error,
            "p_retry": retry, "p_retry_seconds": retry_seconds,
            "p_max_attempts": max_attempts,
        }).execute()
        return bool(res.data)
