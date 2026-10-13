import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient

from .api import tasks_router, tenants_router
from .config import Settings
from .messaging import SqsQueues, run_consumer, run_sweeper
from .store import ApiError, Store

logging.basicConfig(level=logging.INFO)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = AsyncIOMotorClient(settings.mongo_url)
        store = Store(client, settings.db_name)
        await store.ensure_indexes()
        queues = SqsQueues(settings)
        await asyncio.to_thread(queues.ensure_queues)

        app.state.store = store
        app.state.queues = queues

        stop = asyncio.Event()
        background = []
        if settings.background_enabled:
            background = [
                asyncio.create_task(run_sweeper(store, queues, settings.outbox_poll_seconds, stop)),
                asyncio.create_task(run_consumer(store, queues, stop)),
            ]
        try:
            yield
        finally:
            stop.set()
            for task in background:
                await task
            client.close()

    app = FastAPI(title="Tenant Provisioning Control Plane", lifespan=lifespan)
    app.include_router(tenants_router)
    app.include_router(tasks_router)

    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed.",
                    "details": jsonable_encoder(exc.errors()),
                }
            },
        )

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
