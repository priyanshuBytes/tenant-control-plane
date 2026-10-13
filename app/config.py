import os
from dataclasses import dataclass


@dataclass
class Settings:
    mongo_url: str = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    db_name: str = os.getenv("DB_NAME", "control_plane")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    sqs_endpoint_url: str = os.getenv("SQS_ENDPOINT_URL", "")
    tasks_queue: str = os.getenv("TASKS_QUEUE_NAME", "tenant-tasks")
    updates_queue: str = os.getenv("UPDATES_QUEUE_NAME", "tenant-task-updates")
    background_enabled: bool = os.getenv("BACKGROUND_ENABLED", "1") == "1"
    outbox_poll_seconds: float = 1.0
