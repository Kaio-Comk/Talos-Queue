# pg-lease-queue

A small **durable job queue for Postgres / Supabase**: one SQL file plus a tiny
Python client and worker loop. No Redis, no broker.

It is the generic core of the audio-repair queue we run in production for a
Telegram media bot (download, transcode, upload jobs that take minutes and must
survive restarts).

## Guarantees

- **One active job per `(kind, dedupe_key)`**: enqueueing the same work twice
  returns the existing job (partial unique index).
- **Lease + `FOR UPDATE SKIP LOCKED`**: many workers, no double processing. If a
  worker crashes, its job is recovered when the lease expires (it costs one
  attempt, so the attempts cap still applies).
- **Heartbeat**: the worker renews its lease while it works; if it lost the job,
  its result is discarded.
- **Retries with backoff** and a cap (`max_attempts`), or a permanent failure.
- **Locked down**: RLS on, no access for `anon`/`authenticated`; use the service role.

## Install

1. Run `sql/001_job_queue.sql` (staging first).
2. `pip install git+https://github.com/Kaio-Comk/pg-lease-queue.git`

## Use

```python
from pgleasequeue import LeaseQueue, PermanentError, run_worker

queue = LeaseQueue(supabase)            # supabase-py client with the service role key

# producer
queue.enqueue("transcode", dedupe_key="movie:42", payload={"movie_id": 42}, priority=50)

# worker
async def handle(job):
    if not exists(job.payload["movie_id"]):
        raise PermanentError("source missing")      # no retry
    await do_the_work(job.payload)                  # any other exception is retried
    return {"file_id": "..."}                       # stored in job_queue.result

await run_worker(queue, handle, worker_id="worker-1", kind="transcode")
```

`claim`, `heartbeat`, `set_stage`, `finish` and `fail` are plain RPCs if you
prefer to write your own loop (`queue.set_stage(job.id, worker_id, "uploading")`
also renews the lease).

## Status of the SQL

The SQL is a generalisation of a version that has run in production for weeks.
This generic version was syntax-checked with a real PostgreSQL parser
(`pglast`) but has not been load-tested on a database yet: please try it in
staging and open an issue if you find anything.

## Tests

```bash
pip install -e ".[dev]" && pytest
```

## License

MIT
