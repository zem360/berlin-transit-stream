import asyncio
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from loguru import logger

from berlin_departure_board.config import settings
from berlin_departure_board.poller.scheduler import BVGPollingScheduler

scheduler: BVGPollingScheduler
polling_task: asyncio.Task


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler, polling_task

    logger.info("🚀 Starting the poller service...")

    try:
        scheduler = BVGPollingScheduler()

        polling_task = asyncio.create_task(scheduler.start_polling())
        logger.info("✅ Background polling started")

        def signal_handler(signum, frame):
            logger.info(f"📶 Received signal {signum}, initiating shutdown...")
            if polling_task and not polling_task.done():
                polling_task.cancel()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        yield

    except Exception as e:
        logger.error(f"💥 Failed to start poller service: {e}")
        raise

    try:
        if scheduler:
            await scheduler.stop_polling()

        if polling_task and not polling_task.done():
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                logger.info("📋 Polling task cancelled successfully")

        logger.info("✅ Shutdown complete")

    except Exception as e:
        logger.error(f"⚠️  Error during shutdown: {e}")


def create_poller_app() -> FastAPI:
    """Create FastAPI application"""
    app = FastAPI(
        title="BVG API Poller",
        description="FastAPI service that polls BVG API",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "service": "BVG API Poller",
            "status": "running",
            "description": "Continuously polls BVG API",
        }

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        global scheduler

        if not scheduler:
            return {"status": "starting", "message": "Scheduler not yet initialized"}

        return {
            "status": "healthy" if scheduler.is_running else "stopped",
            "service": "bvg-poller",
            "stations_monitored": len(settings.POLLER_ENABLED_STATIONS),
            "polling_interval_seconds": settings.POLLING_INTERVAL,
            "is_polling": scheduler.is_running,
        }

    @app.get("/status")
    async def get_status():
        """Detailed status endpoint"""
        global scheduler, polling_task

        if not scheduler:
            return {"scheduler": "not_initialized", "polling_task": "not_started"}

        status = scheduler.get_status()

        status["polling_task_status"] = (
            "running"
            if polling_task and not polling_task.done()
            else "stopped" if polling_task else "not_started"
        )

        status["config"] = {
            "bvg_base_url": settings.BVG_BASE_URL,
            "rate_limit_delay": settings.BVG_RATE_LIMIT_DELAY,
            "stations": settings.POLLER_ENABLED_STATIONS,
        }

        return status

    @app.post("/stop")
    async def manual_stop():
        """Manually stop polling (for testing)"""
        global scheduler

        if not scheduler or not scheduler.is_running:
            return {"message": "Polling is not running"}

        await scheduler.stop_polling()
        return {"message": "Polling stopped"}

    @app.post("/start")
    async def manual_start():
        """Manually start polling (for testing)"""
        global scheduler, polling_task

        if scheduler and scheduler.is_running:
            return {"message": "Polling is already running"}

        if not scheduler:
            scheduler = BVGPollingScheduler()

        polling_task = asyncio.create_task(scheduler.start_polling())

        return {"message": "Polling started"}

    return app


app = create_poller_app()


def run_poller():
    logger.info(
        f"🌟 Starting BVG Poller on {settings.POLLER_HOST}:{settings.POLLER_PORT}"
    )

    uvicorn.run(
        "berlin_departure_board.poller.main:app",
        host=settings.POLLER_HOST,
        port=settings.POLLER_PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    run_poller()
