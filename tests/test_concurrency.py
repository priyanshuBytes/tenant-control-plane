import asyncio

from conftest import create_tenant, finish_task

PARALLELISM = 8


async def test_concurrent_creates_same_slug_one_winner(api):
    responses = await asyncio.gather(
        *[
            api.post("/tenants", json={"slug": "raced-slug", "name": f"Racer {i}"})
            for i in range(PARALLELISM)
        ]
    )
    statuses = sorted(r.status_code for r in responses)
    assert statuses.count(201) == 1
    assert statuses.count(409) == PARALLELISM - 1
    for r in responses:
        if r.status_code == 409:
            assert r.json()["error"]["code"] == "tenant_already_exists"


async def test_concurrent_patches_same_version_one_winner(api, app):
    tenant = await create_tenant(api)
    await finish_task(app, tenant["id"])

    responses = await asyncio.gather(
        *[
            api.patch(f"/tenants/{tenant['id']}", json={"name": f"Racer {i}", "version": 2})
            for i in range(PARALLELISM)
        ]
    )
    statuses = [r.status_code for r in responses]
    assert statuses.count(200) == 1
    assert statuses.count(409) == PARALLELISM - 1

    resp = await api.get("/tasks", params={"tenant_id": tenant["id"], "status": "accepted"})
    assert resp.json()["total"] == 1
