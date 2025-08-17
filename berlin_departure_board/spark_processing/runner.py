import signal
import sys

from loguru import logger

from berlin_departure_board.config import settings
from berlin_departure_board.spark_processing.processor import BVGSparkProcessor


def signal_handler(sig, frame):
    logger.info("🛑 Received shutdown signal, stopping Spark processor...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("🚀 Starting BVG Spark Streaming Processor")
    logger.info(f"📊 Processing interval: {settings.SPARK_PROCESSING_INTERVAL}")
    logger.info(f"🔗 Kafka servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📡 Kafka topic: {settings.KAFKA_TOPIC_DEPARTURES}")
    logger.info(f"🗄️  Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")

    processor = BVGSparkProcessor()
    queries = None

    try:
        queries = processor.start_processing()

        if isinstance(queries, tuple):
            departure_query, station_query, debug_query, station_storage_query = queries
            departure_query.awaitTermination()
        else:
            queries.awaitTermination()

    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        raise
    finally:
        if queries:
            if isinstance(queries, tuple):
                processor.stop_processing(*queries)
            else:
                processor.stop_processing(queries)
        logger.info("✅ Enhanced Spark processor shutdown complete")


if __name__ == "__main__":
    main()
