import asyncio

from pgleasequeue import Job, LeaseQueue, PermanentError, run_worker


class FakeRPC:
    def __init__(self, data):
        self.data = data

    def execute(self):
        return self


class FakeSB:
    def __init__(self, data=None):
        self.calls, self._data = [], data

    def rpc(self, name, params):
        self.calls.append((name, params))
        return FakeRPC(self._data)


ROW = {"id": 7, "kind": "k", "dedupe_key": "d", "payload": {"a": 1}, "status": "running", "attempts": 2}


def test_enqueue_chama_a_rpc_certa():
    sb = FakeSB(ROW)
    job = LeaseQueue(sb).enqueue("k", "d", {"a": 1}, priority=10)
    assert job == Job(7, "k", "d", {"a": 1}, "running", 2)
    assert sb.calls == [("enqueue_job", {"p_kind": "k", "p_dedupe_key": "d",
                                          "p_payload": {"a": 1}, "p_priority": 10})]


def test_claim_vazio_vira_none():
    # PostgREST returns a composite with every field null when nothing is claimable
    assert LeaseQueue(FakeSB({"id": None, "kind": None})).claim("w") is None
    assert LeaseQueue(FakeSB(None)).claim("w") is None


def test_claim_com_job():
    assert LeaseQueue(FakeSB(ROW)).claim("w", "k").id == 7


class MemQueue:
    """In-memory stand-in with the LeaseQueue interface."""

    def __init__(self, jobs):
        self.jobs, self.done, self.failed = list(jobs), {}, {}
        self.beats = 0

    def claim(self, worker_id, kind=None, lease_seconds=300):
        return self.jobs.pop(0) if self.jobs else None

    def heartbeat(self, job_id, worker_id, lease_seconds=300):
        self.beats += 1
        return True

    def finish(self, job_id, worker_id, result=None):
        self.done[job_id] = result
        return True

    def fail(self, job_id, worker_id, error, *, retry=False, retry_seconds=60, max_attempts=5):
        self.failed[job_id] = (error, retry)
        return True


def _run(q, handler):
    async def go():
        stop = asyncio.Event()

        async def watcher():
            while q.jobs or not (q.done or q.failed):
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(
            run_worker(q, handler, worker_id="w", poll_seconds=0.01, stop=stop),
            watcher())
    asyncio.run(asyncio.wait_for(go(), 5))


def test_worker_conclui_e_guarda_resultado():
    q = MemQueue([Job(1, "k", "a")])

    async def h(job):
        return {"ok": job.id}
    _run(q, h)
    assert q.done == {1: {"ok": 1}} and not q.failed


def test_erro_comum_tenta_de_novo_e_permanente_nao():
    q = MemQueue([Job(1, "k", "a"), Job(2, "k", "b")])

    async def h(job):
        if job.id == 1:
            raise RuntimeError("temporário")
        raise PermanentError("arquivo ausente")
    _run(q, h)
    assert q.failed[1] == ("temporário", True)
    assert q.failed[2] == ("arquivo ausente", False)


def test_lease_perdido_descarta_o_resultado():
    class Lost(MemQueue):
        def heartbeat(self, *a, **k):
            return False

    q = Lost([Job(1, "k", "a")])

    async def run():
        stop = asyncio.Event()
        from pgleasequeue import worker as w
        orig = w._heartbeat

        async def fast(queue, job, wid, lease, lost):
            await asyncio.sleep(0.01)
            lost.set()
        w._heartbeat = fast

        async def h(job):
            await asyncio.sleep(0.1)
            return {"x": 1}
        task = asyncio.create_task(run_worker(q, h, worker_id="w", poll_seconds=0.01, stop=stop))
        await asyncio.sleep(0.4)
        stop.set()
        await task
        w._heartbeat = orig
    asyncio.run(asyncio.wait_for(run(), 5))
    assert q.done == {} and q.failed == {}
