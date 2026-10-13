import os
import uuid

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from moto import mock_aws

from app.config import Settings
from app.main import create_app

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_REGION", "us-east-1")


@pytest.fixture
def settings():
    return Settings(
        mongo_url=os.environ.get("MONGO_URL", "mongodb://localhost:27017/?replicaSet=rs0"),
        db_name=f"test_{uuid.uuid4().hex}",
        sqs_endpoint_url="",
        tasks_queue=f"tasks-{uuid.uuid4().hex[:8]}",
        updates_queue=f"updates-{uuid.uuid4().hex[:8]}",
        background_enabled=False,
    )


@pytest.fixture
async def app(settings):
    with mock_aws():
        application = create_app(settings)
        async with LifespanManager(application):
            yield application
            await application.state.store.client.drop_database(settings.db_name)


@pytest.fixture
async def api(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_tenant(api, slug="acme-corp", name="Acme Corp") -> dict:
    resp = await api.post("/tenants", json={"slug": slug, "name": name})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def latest_task(app, tenant_id: str) -> dict:
    tasks, _ = await app.state.store.list_tasks(tenant_id, None, 1, 0)
    assert tasks, f"no task found for tenant {tenant_id}"
    return tasks[0]


async def finish_task(app, tenant_id: str, outcome: str = "done") -> dict:
    task = await latest_task(app, tenant_id)
    store = app.state.store
    await store.apply_task_update(task["_id"], "in_progress")
    await store.apply_task_update(task["_id"], outcome)
    return task
