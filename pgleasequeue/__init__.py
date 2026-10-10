"""pgleasequeue: durable job queue client for Postgres / Supabase (see sql/001_job_queue.sql)."""
from .client import Job, LeaseQueue
from .worker import PermanentError, run_worker

__all__ = ["Job", "LeaseQueue", "PermanentError", "run_worker"]
__version__ = "0.1.0"
