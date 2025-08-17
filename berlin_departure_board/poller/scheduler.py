import asyncio
from datetime import datetime

from loguru import logger

from berlin_departure_board.config import settings
from berlin_departure_board.kafka_streaming.producer import KafkaProducerClient
from berlin_departure_board.poller.client import BVGAPIClient


class BVGPollingScheduler:
    def __init__(self) -> None:
        self.bvg_client = BVGAPIClient()
        self.kafka_producer = KafkaProducerClient()
        self.is_running = False
        self.stations_to_poll = settings.POLLER_ENABLED_STATIONS

    async def poll_departures_for_station(self, station_id: str) -> bool:
        try:
            logger.debug(f"Polling departures for station {station_id}")

            departures = await self.bvg_client.get_departure_info(
                station_id=station_id, duration_minutes=1
            )

            if departures:
                success = await self.kafka_producer.send_departures(departures)

                if success:
                    logger.info(
                        f"✅ Polled {len(departures)} departures for station {station_id}"
                    )
                    return True
                else:
                    logger.error(
                        f"❌ Failed to send departures for station {station_id}"
                    )
                    return False
            else:
                logger.warning(f"⚠️  No departures found for station {station_id}")
                return False
        except Exception as e:
            logger.error(f"💥 Error polling station {station_id}: {e}")
            return False

    async def polling_cycle(self):
        cycle_start = datetime.now()
        logger.info(
            f"🔄 Starting polling cycle for {len(self.stations_to_poll)} stations"
        )

        successful_polls = 0
        failed_polls = 0

        for station in self.stations_to_poll:
            success = await self.poll_departures_for_station(station)

            if success:
                successful_polls += 1
            else:
                failed_polls += 1

            if self.is_running:
                await asyncio.sleep(settings.BVG_RATE_LIMIT_DELAY)

        cycle_duration = (datetime.now() - cycle_start).total_seconds()

        if successful_polls > 0:
            logger.info(
                f"✅ Cycle complete: {successful_polls} success, {failed_polls} failed in {cycle_duration:.1f}s"
            )
        else:
            logger.error(
                f"❌ Cycle failed: {failed_polls} failures in {cycle_duration:.1f}s"
            )

    async def start_polling(self):
        logger.info("🚀 Starting BVG departure polling...")
        self.is_running = True

        cycle_count = 0
        while self.is_running:
            cycle_count += 1
            logger.debug(f"Starting polling cycle #{cycle_count}")

            try:
                await self.polling_cycle()
                if self.is_running:
                    logger.info(
                        f"😴 Waiting {settings.POLLING_INTERVAL}s until next cycle..."
                    )

                    # Sleep in 1 second intervals instead of complete interval
                    # To check if stop signal is initiated.
                    for _ in range(settings.POLLING_INTERVAL):
                        if not self.is_running:
                            break
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"💥 Error in polling cycle #{cycle_count}: {e}")
                for _ in range(5):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)

    async def stop_polling(self):
        logger.info("🛑 Stopping BVG departure polling...")
        self.is_running = False

        await asyncio.sleep(2)

        try:
            await self.bvg_client.close()
            logger.info("🧹 Resources cleaned up successfully")
        except Exception as e:
            logger.error(f"⚠️  Error during cleanup: {e}")

        logger.info("✅ Polling stopped and resources cleaned up")

    def get_status(self) -> dict:
        """Get current scheduler status"""
        return {
            "is_running": self.is_running,
            "stations_monitored": self.stations_to_poll,
            "polling_interval": settings.POLLING_INTERVAL,
            "total_stations": len(self.stations_to_poll),
        }
