import json

from conftest import create_tenant, finish_task, latest_task

from app.messaging import handle_message, sweep_once


async def test_outbox_publishes_each_task_exactly_once(api, app):
    tenant = await create_tenant(api)
    queues = app.state.queues

    assert await sweep_once(app.state.store, queues) == 1
    messages = queues.receive(queues.tasks_url, wait_seconds=0)
    assert len(messages) == 1
    event = json.loads(messages[0]["Body"])
    task = await latest_task(app, tenant["id"])
    assert event["id"] == task["_id"]
    assert event["type"] == "deploy"
    assert event["tenant_id"] == tenant["id"]
    assert event["status"] == "accepted"

    assert await sweep_once(app.state.store, queues) == 0


async def test_consumer_applies_valid_update(api, app):
    tenant = await create_tenant(api)
    task = await latest_task(app, tenant["id"])

    body = json.dumps({"id": "msg-1", "task_id": task["_id"], "status": "in_progress"})
    assert await handle_message(app.state.store, body) is True

    resp = await api.get(f"/tasks/{task['_id']}")
    assert resp.json()["status"] == "in_progress"


async def test_duplicate_update_is_noop(api, app):
    tenant = await create_tenant(api)
    task = await finish_task(app, tenant["id"])
    store = app.state.store

    assert await store.apply_task_update(task["_id"], "done") is False

    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "active"
    assert resp.json()["version"] == 2


async def test_stale_update_does_not_regress(api, app):
    tenant = await create_tenant(api)
    task = await finish_task(app, tenant["id"])
    store = app.state.store

    assert await store.apply_task_update(task["_id"], "in_progress") is False

    resp = await api.get(f"/tasks/{task['_id']}")
    assert resp.json()["status"] == "done"
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "active"


async def test_terminal_outcome_arriving_before_in_progress(api, app):
    tenant = await create_tenant(api)
    task = await latest_task(app, tenant["id"])
    store = app.state.store

    assert await store.apply_task_update(task["_id"], "done") is True
    assert await store.apply_task_update(task["_id"], "in_progress") is False

    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "active"


async def test_update_for_unknown_task_is_noop(app):
    assert await app.state.store.apply_task_update("no-such-task", "done") is False


async def test_malformed_messages_do_not_raise(app):
    store = app.state.store
    assert await handle_message(store, "this is not json") is False
    assert await handle_message(store, json.dumps({"unrelated": True})) is False
    assert await handle_message(store, json.dumps({"task_id": "x", "status": "exploded"})) is False
