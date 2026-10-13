import uuid
from datetime import UTC, datetime

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

# --- state machines ---

TASK_TYPES = ("deploy", "update", "destroy")
TASK_STATUSES = ("accepted", "in_progress", "done", "failed")
TASK_TERMINAL = {"done", "failed"}

# updates may only move a task forward in rank
TASK_RANK = {"accepted": 0, "in_progress": 1, "done": 2, "failed": 2}

# task type -> tenant status while that task is open
OPEN_TASK_TENANT_STATUS = {
    "deploy": "provisioning",
    "update": "updating",
    "destroy": "destroying",
}

# (task type, terminal task status) -> resulting tenant status
TENANT_STATUS_AFTER_TASK = {
    ("deploy", "done"): "active",
    ("deploy", "failed"): "failed",
    ("update", "done"): "active",
    ("update", "failed"): "failed",
    ("destroy", "done"): "destroyed",
    ("destroy", "failed"): "failed",
}

PATCH_ALLOWED_FROM = {"active"}
DELETE_ALLOWED_FROM = {"active", "failed"}


def prior_task_statuses(new_status: str) -> list[str]:
    if new_status not in TASK_RANK:
        raise ValueError(f"unknown task status: {new_status}")
    rank = TASK_RANK[new_status]
    return [s for s in TASK_STATUSES if TASK_RANK[s] < rank and s not in TASK_TERMINAL]


class ApiError(Exception):
    def __init__(self, code: str, message: str, status: int):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return uuid.uuid4().hex


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class Store:
    def __init__(self, client: AsyncIOMotorClient, db_name: str):
        self.client = client
        db = client[db_name]
        self.tenants = db.tenants
        self.tasks = db.tasks

    async def ensure_indexes(self) -> None:
        await self.tenants.create_index("slug", unique=True)
        await self.tasks.create_index("tenant_id")
        await self.tasks.create_index([("published", 1), ("created_at", 1)])

    async def create_tenant(self, slug: str, name: str) -> dict:
        now = utcnow()
        tenant = {
            "_id": new_id(),
            "slug": slug,
            "name": name,
            "status": "provisioning",
            "version": 1,
            "created_at": now,
            "updated_at": now,
        }
        task = self._new_task("deploy", tenant["_id"], now)

        async def txn(session):
            await self.tenants.insert_one(tenant, session=session)
            await self.tasks.insert_one(task, session=session)

        try:
            await self._transaction(txn)
        except DuplicateKeyError:
            raise ApiError(
                "tenant_already_exists", f"Tenant with slug '{slug}' already exists.", 409
            ) from None
        return tenant

    async def patch_tenant(self, tenant_id: str, name: str | None, version: int) -> dict:
        now = utcnow()
        update = {"$set": {"status": "updating", "updated_at": now}, "$inc": {"version": 1}}
        if name is not None:
            update["$set"]["name"] = name

        async def txn(session):
            tenant = await self.tenants.find_one_and_update(
                {
                    "_id": tenant_id,
                    "version": version,
                    "status": {"$in": list(PATCH_ALLOWED_FROM)},
                },
                update,
                session=session,
                return_document=ReturnDocument.AFTER,
            )
            if tenant is not None:
                await self.tasks.insert_one(
                    self._new_task("update", tenant_id, now), session=session
                )
            return tenant

        tenant = await self._transaction(txn)
        if tenant is None:
            current = await self.tenants.find_one({"_id": tenant_id})
            if current is None:
                raise self._not_found()
            if current["status"] not in PATCH_ALLOWED_FROM:
                raise self._not_allowed(current["status"])
            raise ApiError(
                "tenant_version_conflict",
                f"Version {version} does not match current version {current['version']}.",
                409,
            )
        return tenant

    async def delete_tenant(self, tenant_id: str) -> dict:
        now = utcnow()

        async def txn(session):
            tenant = await self.tenants.find_one_and_update(
                {"_id": tenant_id, "status": {"$in": list(DELETE_ALLOWED_FROM)}},
                {"$set": {"status": "destroying", "updated_at": now}, "$inc": {"version": 1}},
                session=session,
                return_document=ReturnDocument.AFTER,
            )
            if tenant is not None:
                await self.tasks.insert_one(
                    self._new_task("destroy", tenant_id, now), session=session
                )
            return tenant

        tenant = await self._transaction(txn)
        if tenant is None:
            current = await self.tenants.find_one({"_id": tenant_id})
            if current is None:
                raise self._not_found()
            raise self._not_allowed(current["status"])
        return tenant

    async def get_tenant(self, tenant_id: str) -> dict:
        tenant = await self.tenants.find_one({"_id": tenant_id})
        if tenant is None:
            raise self._not_found()
        return tenant

    async def list_tenants(self, status: str | None, limit: int, offset: int) -> tuple[list, int]:
        query = {"status": status} if status else {}
        total = await self.tenants.count_documents(query)
        cursor = self.tenants.find(query).sort("created_at", -1).skip(offset).limit(limit)
        return await cursor.to_list(length=limit), total

    async def get_task(self, task_id: str) -> dict:
        task = await self.tasks.find_one({"_id": task_id})
        if task is None:
            raise ApiError("task_not_found", "Task does not exist.", 404)
        return task

    async def list_tasks(
        self, tenant_id: str | None, status: str | None, limit: int, offset: int
    ) -> tuple[list, int]:
        query = {}
        if tenant_id:
            query["tenant_id"] = tenant_id
        if status:
            query["status"] = status
        total = await self.tasks.count_documents(query)
        cursor = self.tasks.find(query).sort("created_at", -1).skip(offset).limit(limit)
        return await cursor.to_list(length=limit), total

    async def apply_task_update(self, task_id: str, new_status: str) -> bool:
        """Apply a worker progress update; duplicate or out-of-order updates
        are no-ops. Returns True if the update was applied."""
        allowed_prior = prior_task_statuses(new_status)
        if not allowed_prior:
            return False
        now = utcnow()

        async def txn(session):
            task = await self.tasks.find_one_and_update(
                {"_id": task_id, "status": {"$in": allowed_prior}},
                {"$set": {"status": new_status, "updated_at": now}},
                session=session,
                return_document=ReturnDocument.AFTER,
            )
            if task is None:
                return False
            if new_status in TASK_TERMINAL:
                await self.tenants.update_one(
                    {
                        "_id": task["tenant_id"],
                        "status": OPEN_TASK_TENANT_STATUS[task["type"]],
                    },
                    {
                        "$set": {
                            "status": TENANT_STATUS_AFTER_TASK[(task["type"], new_status)],
                            "updated_at": now,
                        },
                        "$inc": {"version": 1},
                    },
                    session=session,
                )
            return True

        return await self._transaction(txn)

    async def fetch_unpublished_tasks(self, limit: int = 20) -> list[dict]:
        cursor = self.tasks.find({"published": False}).sort("created_at", 1).limit(limit)
        return await cursor.to_list(length=limit)

    async def mark_published(self, task_id: str) -> None:
        await self.tasks.update_one({"_id": task_id}, {"$set": {"published": True}})

    async def _transaction(self, callback):
        # with_transaction retries transient write conflicts
        async with await self.client.start_session() as session:
            return await session.with_transaction(callback)

    @staticmethod
    def _new_task(task_type: str, tenant_id: str, now: datetime) -> dict:
        return {
            "_id": new_id(),
            "type": task_type,
            "tenant_id": tenant_id,
            "status": "accepted",
            "published": False,
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def _not_found() -> ApiError:
        return ApiError("tenant_not_found", "Tenant does not exist.", 404)

    @staticmethod
    def _not_allowed(status: str) -> ApiError:
        return ApiError(
            "tenant_update_not_allowed", f"Operation not allowed in status '{status}'.", 409
        )
