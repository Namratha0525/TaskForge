"""Exercise the API and worker against an already running TaskForge instance."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

BASE = os.environ.get("TASKFORGE_URL", "http://127.0.0.1:8081")
TOKEN = os.environ.get("TASKFORGE_TOKEN", "local-taskforge-demo-token")


def call(method, path, statuses=(200,), body=None, key=None, auth=True):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = f"Bearer {TOKEN}"
    if key:
        headers["Idempotency-Key"] = key
    data = None if body is None else json.dumps(body).encode()
    try:
        response = urlopen(Request(BASE + path, data, headers, method=method), timeout=10)
    except HTTPError as error:
        response = error
    with response:
        assert response.status in statuses, (path, response.status, response.read())
        return response.status, json.load(response)


def submit(kind, priority=5, max_attempts=3, key=None):
    return call("POST", "/jobs", (200, 202),
                {"kind": kind, "payload": "demo-42", "priority": priority,
                 "maxAttempts": max_attempts}, key or str(uuid4()))


def wait_state(job_id, state, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, details = call("GET", f"/jobs/{job_id}")
        if details["job"]["state"] == state:
            return details
        time.sleep(0.1)
    raise AssertionError(f"{job_id} never reached {state}")


def main():
    call("GET", "/jobs", (401,), auth=False)
    call("POST", "/jobs", (400,), {"kind": "INVOICE", "payload": "demo",
         "priority": 10, "maxAttempts": 3}, str(uuid4()))
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = [pool.submit(submit, "INVOICE", key=key) for _ in range(4)]
        submissions = [future.result() for future in pending]
    assert sorted(status for status, _ in submissions) == [200, 200, 200, 202]
    ids = {job["id"] for _, job in submissions}
    assert len(ids) == 1
    call("POST", "/jobs", (409,), {"kind": "REPORT", "payload": "other",
         "priority": 5, "maxAttempts": 3}, key)
    assert "invoice" in wait_state(ids.pop(), "COMPLETED")["result"]
    _, retry = submit("RETRY_DEMO")
    assert wait_state(retry["id"], "COMPLETED")["job"]["attempts"] == 2
    _, terminal = submit("RETRY_DEMO", max_attempts=1)
    assert wait_state(terminal["id"], "FAILED")["job"]["attempts"] == 1
    assert any(item["id"] == terminal["id"]
               for item in call("GET", "/jobs?state=FAILED&limit=100")[1])
    assert any(item["state"] == "COMPLETED" for item in call("GET", "/stats")[1])
    print("Authentication, validation, duplicate jobs, retries, terminal failure and monitoring passed.")


if __name__ == "__main__":
    main()
