"""SQS queues, outbox sweeper and progress update consumer."""

import asyncio
import contextlib
import json
import logging

import boto3

from .config import Settings
from .store import TASK_RANK, Store, iso

log = logging.getLogger(__name__)

MAX_RECEIVES = 5


class SqsQueues:
    def __init__(self, settings: Settings):
        self.client = boto3.client(
            "sqs",
            region_name=settings.aws_region,
            endpoint_url=settings.sqs_endpoint_url or None,
        )
        self.settings = settings
        self.tasks_url = ""
        self.updates_url = ""

    def ensure_queues(self) -> None:
        self.tasks_url = self._create_with_dlq(self.settings.tasks_queue)
        self.updates_url = self._create_with_dlq(self.settings.updates_queue)

    def _create_with_dlq(self, name: str) -> str:
        dlq_url = self.client.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
        dlq_arn = self.client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": MAX_RECEIVES})
        return self.client.create_queue(QueueName=name, Attributes={"RedrivePolicy": redrive})[
            "QueueUrl"
        ]

    def publish_task(self, event: dict) -> None:
        self.client.send_message(QueueUrl=self.tasks_url, MessageBody=json.dumps(event))

    def publish_update(self, update: dict) -> None:
        self.client.send_message(QueueUrl=self.updates_url, MessageBody=json.dumps(update))

    def receive(self, queue_url: str, wait_seconds: int = 10) -> list[dict]:
        resp = self.client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=wait_seconds
        )
        return resp.get("Messages", [])

    def delete(self, queue_url: str, receipt_handle: str) -> None:
        self.client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def task_event(task: dict) -> dict:
    return {
        "id": task["_id"],
        "type": task["type"],
        "tenant_id": task["tenant_id"],
        "status": task["status"],
        "created_at": iso(task["created_at"]),
    }


async def sweep_once(store: Store, queues: SqsQueues) -> int:
    published = 0
    for task in await store.fetch_unpublished_tasks():
        await asyncio.to_thread(queues.publish_task, task_event(task))
        await store.mark_published(task["_id"])
        published += 1
    return published


async def run_sweeper(store: Store, queues: SqsQueues, poll: float, stop: asyncio.Event) -> None:
    delay = poll
    while not stop.is_set():
        try:
            await sweep_once(store, queues)
            delay = poll
        except Exception:
            log.exception("outbox sweep failed, backing off %.1fs", delay)
            delay = min(delay * 2, 30)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


async def handle_message(store: Store, body: str) -> bool:
    """Returns True if the message can be deleted, False if malformed."""
    try:
        update = json.loads(body)
        task_id = update["task_id"]
        new_status = update["status"]
    except (ValueError, TypeError, KeyError):
        log.warning("malformed update message: %.200s", body)
        return False
    if new_status not in TASK_RANK:
        log.warning("unknown status %r for task %s", new_status, task_id)
        return False
    applied = await store.apply_task_update(task_id, new_status)
    if not applied:
        log.info("ignored update %s -> %s (duplicate, stale or unknown)", task_id, new_status)
    return True


async def run_consumer(store: Store, queues: SqsQueues, stop: asyncio.Event) -> None:
    delay = 1.0
    while not stop.is_set():
        try:
            messages = await asyncio.to_thread(queues.receive, queues.updates_url, 5)
            delay = 1.0
        except Exception:
            log.exception("receive failed, backing off %.1fs", delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
            delay = min(delay * 2, 30)
            continue

        for message in messages:
            try:
                ok = await handle_message(store, message["Body"])
            except Exception:
                log.exception("failed to process update, leaving for redelivery")
                continue
            if ok:
                await asyncio.to_thread(queues.delete, queues.updates_url, message["ReceiptHandle"])
