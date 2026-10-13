"""Worker simulator: consumes tasks and reports progress back through SQS.

Usage: python -m worker.simulator [--fail-rate 0.5] [--min-delay-ms 100] [--max-delay-ms 500]
"""

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import UTC, datetime

from app.config import Settings
from app.messaging import SqsQueues

log = logging.getLogger("worker")


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-rate", type=float, default=float(os.getenv("FAIL_RATE", "0")))
    parser.add_argument("--min-delay-ms", type=int, default=int(os.getenv("MIN_DELAY_MS", "500")))
    parser.add_argument("--max-delay-ms", type=int, default=int(os.getenv("MAX_DELAY_MS", "2000")))
    return parser.parse_args(argv)


def make_update(task_id: str, status: str) -> dict:
    return {
        "id": uuid.uuid4().hex,
        "task_id": task_id,
        "status": status,
        "occurred_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
    }


def process_task(queues: SqsQueues, body: str, args) -> None:
    task = json.loads(body)
    task_id = task["id"]
    log.info("picked up %s task %s for tenant %s", task["type"], task_id, task["tenant_id"])

    queues.publish_update(make_update(task_id, "in_progress"))

    delay_ms = random.randint(args.min_delay_ms, args.max_delay_ms)  # nosec B311
    time.sleep(delay_ms / 1000)

    outcome = "failed" if random.random() < args.fail_rate else "done"  # nosec B311
    queues.publish_update(make_update(task_id, outcome))
    log.info("task %s -> %s after %dms", task_id, outcome, delay_ms)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    queues = SqsQueues(Settings())
    queues.ensure_queues()
    log.info(
        "worker started: fail_rate=%.2f delay=%d-%dms",
        args.fail_rate,
        args.min_delay_ms,
        args.max_delay_ms,
    )

    while True:
        try:
            messages = queues.receive(queues.tasks_url, wait_seconds=10)
        except Exception:
            log.exception("receive failed, retrying in 3s")
            time.sleep(3)
            continue
        for message in messages:
            try:
                process_task(queues, message["Body"], args)
            except Exception:
                log.exception("bad task message, leaving for redelivery")
                continue
            queues.delete(queues.tasks_url, message["ReceiptHandle"])


if __name__ == "__main__":
    main()
