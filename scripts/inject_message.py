"""Publish update messages directly onto the broker, for testing duplicate,
out-of-order and poison deliveries. See README for examples."""

import argparse
import json
import uuid

from app.config import Settings
from app.messaging import SqsQueues


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id")
    parser.add_argument("--status", choices=["accepted", "in_progress", "done", "failed"])
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--raw", help="send this exact body instead of a structured update")
    args = parser.parse_args()
    if args.raw is None and not (args.task_id and args.status):
        parser.error("--task-id and --status are required unless --raw is used")

    queues = SqsQueues(Settings())
    queues.ensure_queues()

    for _ in range(args.count):
        if args.raw is not None:
            queues.client.send_message(QueueUrl=queues.updates_url, MessageBody=args.raw)
            print(f"sent raw: {args.raw!r}")
        else:
            update = {"id": uuid.uuid4().hex, "task_id": args.task_id, "status": args.status}
            queues.publish_update(update)
            print(f"sent: {json.dumps(update)}")


if __name__ == "__main__":
    main()
