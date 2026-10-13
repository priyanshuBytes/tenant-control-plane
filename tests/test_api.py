import re

from conftest import create_tenant, finish_task, latest_task

ISO_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


async def test_full_lifecycle(api, app):
    tenant = await create_tenant(api)
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert ISO_MS_Z.match(tenant["created_at"])
    assert ISO_MS_Z.match(tenant["updated_at"])

    resp = await api.get("/tasks", params={"tenant_id": tenant["id"]})
    tasks = resp.json()["items"]
    assert [t["type"] for t in tasks] == ["deploy"]
    assert tasks[0]["status"] == "accepted"

    await finish_task(app, tenant["id"])
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "active"
    assert resp.json()["version"] == 2

    resp = await api.patch(f"/tenants/{tenant['id']}", json={"name": "Acme v2", "version": 2})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "updating"
    assert body["name"] == "Acme v2"
    assert body["version"] == 3
    assert (await latest_task(app, tenant["id"]))["type"] == "update"

    await finish_task(app, tenant["id"])
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "active"

    resp = await api.delete(f"/tenants/{tenant['id']}")
    assert resp.status_code == 202
    assert resp.json()["status"] == "destroying"
    assert (await latest_task(app, tenant["id"]))["type"] == "destroy"

    await finish_task(app, tenant["id"])
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "destroyed"


async def test_failed_deploy_then_destroy(api, app):
    tenant = await create_tenant(api, slug="doomed-tenant")
    await finish_task(app, tenant["id"], outcome="failed")
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "failed"

    resp = await api.delete(f"/tenants/{tenant['id']}")
    assert resp.status_code == 202
    await finish_task(app, tenant["id"])
    resp = await api.get(f"/tenants/{tenant['id']}")
    assert resp.json()["status"] == "destroyed"


async def test_duplicate_slug_rejected(api):
    await create_tenant(api, slug="taken-slug")
    resp = await api.post("/tenants", json={"slug": "taken-slug", "name": "Other"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_already_exists"


async def test_slug_stays_taken_after_destroy(api, app):
    tenant = await create_tenant(api, slug="forever-taken")
    await finish_task(app, tenant["id"])
    await api.delete(f"/tenants/{tenant['id']}")
    await finish_task(app, tenant["id"])

    resp = await api.post("/tenants", json={"slug": "forever-taken", "name": "Reuse"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_already_exists"


async def test_invalid_slug_rejected(api):
    for bad in ["ab", "-starts-with-dash", "UPPER", "ends-with-dash-", "a" * 29]:
        resp = await api.post("/tenants", json={"slug": bad, "name": "X"})
        assert resp.status_code == 422, bad
        assert resp.json()["error"]["code"] == "validation_error"


async def test_tenant_not_found(api):
    resp = await api.get("/tenants/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "tenant_not_found"

    resp = await api.patch("/tenants/nope", json={"version": 1})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "tenant_not_found"

    resp = await api.delete("/tenants/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "tenant_not_found"


async def test_task_not_found(api):
    resp = await api.get("/tasks/nope")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "task_not_found"


async def test_patch_only_allowed_from_active(api):
    tenant = await create_tenant(api)
    resp = await api.patch(f"/tenants/{tenant['id']}", json={"name": "X", "version": 1})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_update_not_allowed"


async def test_patch_version_conflict(api, app):
    tenant = await create_tenant(api)
    await finish_task(app, tenant["id"])
    resp = await api.patch(f"/tenants/{tenant['id']}", json={"name": "X", "version": 1})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_version_conflict"


async def test_delete_only_allowed_from_active_or_failed(api):
    tenant = await create_tenant(api)
    resp = await api.delete(f"/tenants/{tenant['id']}")
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "tenant_update_not_allowed"


async def test_list_filters_and_pagination(api, app):
    first = await create_tenant(api, slug="list-first")
    await create_tenant(api, slug="list-second")
    await finish_task(app, first["id"])

    resp = await api.get("/tenants", params={"status": "active"})
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["slug"] == "list-first"

    resp = await api.get("/tenants", params={"limit": 1, "offset": 1})
    body = resp.json()
    assert body["total"] == 2
    assert len(body["items"]) == 1
