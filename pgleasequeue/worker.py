"""Async worker loop: claim -> handler (with background heartbeat) -> finish / fail."""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from .client import Job, LeaseQueue

log = logging.getLogger(__name__)


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (missing input, invalid data)."""


async def _heartbeat(queue: LeaseQueue, job: Job, worker_id: str,
                     lease_seconds: int, lost: asyncio.Event) -> None:
    interval = max(5.0, lease_seconds / 3)
    while True:
        await asyncio.sleep(interval)
        ok = await asyncio.to_thread(queue.heartbeat, job.id, worker_id, lease_seconds)
        if not ok:
            lost.set()          # another worker owns the job now
            return


async def run_worker(queue: LeaseQueue, handler: Callable[[Job], Awaitable[Optional[dict]]], *,
                     worker_id: str, kind: Optional[str] = None, lease_seconds: int = 300,
                     poll_seconds: float = 5.0, retry_seconds: int = 60,
                     max_attempts: int = 5, stop: Optional[asyncio.Event] = None) -> None:
    """Process jobs until `stop` is set.

    `handler(job)` may return a dict stored as the job's result. Any exception
    except `PermanentError` is retried with backoff (up to `max_attempts`);
    `PermanentError` fails the job at once. If the lease is lost mid-run the
    result is discarded (the new owner will finish the job).
    """
    stop = stop or asyncio.Event()
    while not stop.is_set():
        try:
            job = await asyncio.to_thread(queue.claim, worker_id, kind, lease_seconds)
        except Exception as exc:  # noqa: BLE001 - the loop must survive a DB blip
            log.warning("claim failed (%s); backing off", exc)
            await _sleep(stop, poll_seconds * 2)
            continue
        if job is None:
            await _sleep(stop, poll_seconds)
            continue

        lost = asyncio.Event()
        beat = asyncio.create_task(_heartbeat(queue, job, worker_id, lease_seconds, lost))
        try:
            result = await handler(job)
        except PermanentError as exc:
            await _settle(queue.fail, job.id, worker_id, str(exc), retry=False)
        except asyncio.CancelledError:
            beat.cancel()
            raise
        except Exception as exc:  # noqa: BLE001 - any handler failure is retried
            log.warning("job %s failed: %s", job.id, exc)
            await _settle(queue.fail, job.id, worker_id, str(exc), retry=True,
                          retry_seconds=retry_seconds, max_attempts=max_attempts)
        else:
            if lost.is_set():
                log.warning("job %s: lease lost, discarding result", job.id)
            else:
                await _settle(queue.finish, job.id, worker_id, result)
        finally:
            beat.cancel()
            await asyncio.gather(beat, return_exceptions=True)


async def _settle(fn, *args, **kwargs) -> None:
    try:
        await asyncio.to_thread(lambda: fn(*args, **kwargs))
    except Exception as exc:  # noqa: BLE001 - the lease expiry recovers the job
        log.warning("could not record job outcome (%s); lease expiry will recover it", exc)


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
