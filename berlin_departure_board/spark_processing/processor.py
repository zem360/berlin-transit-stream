import json
from datetime import datetime
from typing import Optional

import redis  # type: ignore[import]
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from berlin_departure_board.config import settings


class BVGSparkProcessor:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        logger.info(
            f"✅ Sync Redis client initialized: {settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )

        self.kafka_config = {
            "kafka.bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": settings.KAFKA_TOPIC_DEPARTURES,
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
        }

        self.spark = (
            SparkSession.builder.appName(settings.SPARK_APP_NAME)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            )
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session initialized")

        self.message_schema = StructType(
            [
                StructField("event_type", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField(
                    "data",
                    StructType(
                        [
                            StructField("trip_id", StringType(), True),
                            StructField("station_id", StringType(), True),
                            StructField("station_name", StringType(), True),
                            StructField("line_id", StringType(), True),
                            StructField("line_name", StringType(), True),
                            StructField("transport_mode", StringType(), True),
                            StructField("direction", StringType(), True),
                            StructField("planned_departure", StringType(), True),
                            StructField("actual_departure", StringType(), True),
                            StructField("delay_seconds", DoubleType(), True),
                            StructField("longitude", DoubleType(), True),
                            StructField("latitude", DoubleType(), True),
                            StructField("platform", StringType(), True),
                            StructField("planned_platform", StringType(), True),
                            StructField("collected_at", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def create_kafka_stream(self):
        logger.info("📡 Connecting to Kafka stream...")

        return self.spark.readStream.format("kafka").options(**self.kafka_config).load()

    def parse_and_filter_messages(self, kafka_df):
        logger.info("🔍 Setting up message parsing and filtering...")

        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), self.message_schema).alias(
                "parsed_data"
            ),
        ).select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("parsed_data.event_type"),
            col("parsed_data.data.*"),
        )

        filtered_df = (
            parsed_df.withColumn(
                "planned_departure_ts", to_timestamp(col("planned_departure"))
            )
            .withColumn("actual_departure_ts", to_timestamp(col("actual_departure")))
            .withColumn("collected_at_ts", to_timestamp(col("collected_at")))
            .withColumn("current_time", current_timestamp())
            .filter(col("planned_departure_ts") > col("current_time"))
            .filter(col("station_id").isNotNull())
            .filter(col("trip_id").isNotNull())
            .filter(col("line_id").isNotNull())
        )

        return filtered_df

    def store_departure_sync(self, departure_data: dict) -> bool:
        """Synchronous version of departure storage for Spark context"""
        try:
            station_id = departure_data["station_id"]

            collected_at = departure_data.get("collected_at_ts")
            if collected_at:
                if isinstance(collected_at, str):
                    collected_at = datetime.fromisoformat(
                        collected_at.replace("Z", "+00:00")
                    )
                score = collected_at.timestamp()
            else:
                score = datetime.now().timestamp()

            redis_data = {
                "trip_id": departure_data["trip_id"],
                "station_id": station_id,
                "station_name": departure_data["station_name"],
                "line_id": departure_data["line_id"],
                "line_name": departure_data["line_name"],
                "transport_mode": departure_data["transport_mode"],
                "direction": departure_data["direction"],
                "planned_departure": departure_data["planned_departure"],
                "actual_departure": departure_data["actual_departure"],
                "delay_minutes": departure_data.get("delay_seconds", 0.0),
                "platform": departure_data.get("platform"),
                "planned_platform": departure_data.get("planned_platform"),
                "collected_at": departure_data["collected_at"],
                "longitude": departure_data.get("longitude"),
                "latitude": departure_data.get("latitude"),
            }

            key = f"{settings.REDIS_DEPARTURES_KEY_PREFIX}station:{station_id}"

            self.redis_client.zadd(key, {json.dumps(redis_data, default=str): score})

            self.redis_client.expire(key, settings.REDIS_TTL_SECONDS)
            self.redis_client.zremrangebyrank(key, 0, -51)

            return True

        except Exception as e:
            logger.error(f"Failed to store departure in Redis: {e}")
            return False

    def write_to_redis_foreach_batch(self, df, epoch_id):
        batch_count = df.count()
        if batch_count == 0:
            logger.debug(f"📦 Batch {epoch_id}: No new departures to process")
            return

        logger.info(f"📦 Processing batch {epoch_id} with {batch_count} departures")

        try:
            self.redis_client.ping()

            departures = df.collect()
            stored_count = 0
            error_count = 0

            for row in departures:
                try:
                    departure_data = row.asDict()

                    if self.store_departure_sync(departure_data):
                        stored_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    logger.warning(f"Failed to process row in batch {epoch_id}: {e}")
                    error_count += 1
                    continue

            logger.info(
                f"✅ Batch {epoch_id}: Stored {stored_count}/{batch_count} departures (errors: {error_count})"
            )

        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed for batch {epoch_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error processing batch {epoch_id}: {e}")

    def start_processing(self):
        try:
            logger.info("🚀 Starting BVG departure processing pipeline...")

            kafka_stream = self.create_kafka_stream()

            filtered_stream = self.parse_and_filter_messages(kafka_stream)

            output_stream = filtered_stream.select(
                "trip_id",
                "station_id",
                "station_name",
                "line_id",
                "line_name",
                "transport_mode",
                "direction",
                "planned_departure",
                "actual_departure",
                "delay_seconds",
                "platform",
                "planned_platform",
                "collected_at",
                "collected_at_ts",
                "longitude",
                "latitude",
            )

            query = (
                output_stream.writeStream.foreachBatch(
                    self.write_to_redis_foreach_batch
                )
                .outputMode("append")
                .option("checkpointLocation", "/tmp/spark-checkpoint/bvg-departures")
                .trigger(processingTime=settings.SPARK_PROCESSING_INTERVAL)
                .start()
            )

            logger.info("✅ Streaming query started successfully!")
            logger.info(f"📊 Processing interval: {settings.SPARK_PROCESSING_INTERVAL}")
            logger.info(
                f"🔑 Redis key pattern: {settings.REDIS_DEPARTURES_KEY_PREFIX}station:{{station_id}}"
            )
            logger.info("🎯 Only future departures will be stored in Redis")

            return query

        except Exception as e:
            logger.error(f"❌ Failed to start processing: {e}")
            raise

    def stop_processing(self, query):
        """Stop the streaming query and close connections"""
        logger.info("🛑 Stopping streaming query...")

        if query and query.isActive:
            query.stop()
            logger.info("✅ Streaming query stopped")

        try:
            if self.redis_client:
                self.redis_client.close()
                logger.info("✅ Redis connection closed")
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")

        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session closed")
