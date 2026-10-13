from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from .store import Store, iso

SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"


class TenantCreate(BaseModel):
    slug: str = Field(pattern=SLUG_PATTERN, min_length=3, max_length=28)
    name: str = Field(min_length=1)


class TenantPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    version: int = Field(ge=1)


class TenantOut(BaseModel):
    id: str
    slug: str
    name: str
    status: str
    version: int
    created_at: str
    updated_at: str


class TaskOut(BaseModel):
    id: str
    type: str
    tenant_id: str
    status: str
    created_at: str
    updated_at: str


class TenantList(BaseModel):
    items: list[TenantOut]
    total: int
    limit: int
    offset: int


class TaskList(BaseModel):
    items: list[TaskOut]
    total: int
    limit: int
    offset: int


def tenant_out(doc: dict) -> TenantOut:
    return TenantOut(
        id=doc["_id"],
        slug=doc["slug"],
        name=doc["name"],
        status=doc["status"],
        version=doc["version"],
        created_at=iso(doc["created_at"]),
        updated_at=iso(doc["updated_at"]),
    )


def task_out(doc: dict) -> TaskOut:
    return TaskOut(
        id=doc["_id"],
        type=doc["type"],
        tenant_id=doc["tenant_id"],
        status=doc["status"],
        created_at=iso(doc["created_at"]),
        updated_at=iso(doc["updated_at"]),
    )


tenants_router = APIRouter(prefix="/tenants", tags=["tenants"])
tasks_router = APIRouter(prefix="/tasks", tags=["tasks"])


def get_store(request: Request) -> Store:
    return request.app.state.store


@tenants_router.post("", status_code=201, response_model=TenantOut)
async def create_tenant(body: TenantCreate, request: Request):
    tenant = await get_store(request).create_tenant(body.slug, body.name)
    return tenant_out(tenant)


@tenants_router.get("", response_model=TenantList)
async def list_tenants(
    request: Request,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    docs, total = await get_store(request).list_tenants(status, limit, offset)
    return TenantList(items=[tenant_out(d) for d in docs], total=total, limit=limit, offset=offset)


@tenants_router.get("/{tenant_id}", response_model=TenantOut)
async def get_tenant(tenant_id: str, request: Request):
    return tenant_out(await get_store(request).get_tenant(tenant_id))


@tenants_router.patch("/{tenant_id}", response_model=TenantOut)
async def patch_tenant(tenant_id: str, body: TenantPatch, request: Request):
    tenant = await get_store(request).patch_tenant(tenant_id, body.name, body.version)
    return tenant_out(tenant)


@tenants_router.delete("/{tenant_id}", status_code=202, response_model=TenantOut)
async def delete_tenant(tenant_id: str, request: Request):
    return tenant_out(await get_store(request).delete_tenant(tenant_id))


@tasks_router.get("", response_model=TaskList)
async def list_tasks(
    request: Request,
    tenant_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    docs, total = await get_store(request).list_tasks(tenant_id, status, limit, offset)
    return TaskList(items=[task_out(d) for d in docs], total=total, limit=limit, offset=offset)


@tasks_router.get("/{task_id}", response_model=TaskOut)
async def get_task(task_id: str, request: Request):
    return task_out(await get_store(request).get_task(task_id))
