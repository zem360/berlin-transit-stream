import shutil
from datetime import datetime
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    current_timestamp,
    from_json,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, when, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from berlin_departure_board.config import settings
from berlin_departure_board.storage.client import RedisClient


class BVGSparkProcessor:
    def __init__(self):
        self.redis_client = RedisClient()
        logger.info("✅ Enhanced Redis client initialized for Spark processing")
        self._cleanup_checkpoints()

        self.kafka_config = {
            "kafka.bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": settings.KAFKA_TOPIC_DEPARTURES,
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
        }

        self.spark = (
            SparkSession.builder.appName(settings.SPARK_APP_NAME)  # type: ignore
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

        self.window_duration = "15 minutes"
        self.watermark_delay = "10 minutes"

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
                            StructField("delay_minutes", DoubleType(), True),
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
            .withColumn("delay_minutes", col("delay_minutes"))
            .withColumn("is_delayed", when(col("delay_minutes") > 2, 1).otherwise(0))
            .withColumn("is_on_time", when(col("delay_minutes") <= 2, 1).otherwise(0))
            .withColumn(
                "is_significant_delay", when(col("delay_minutes") > 5, 1).otherwise(0)
            )
            .filter(col("station_id").isNotNull())
            .filter(col("trip_id").isNotNull())
            .filter(col("line_id").isNotNull())
            .filter(col("collected_at_ts").isNotNull())
            .withWatermark("collected_at_ts", self.watermark_delay)
        )

        return filtered_df

    def debug_aggregation_foreach_batch(self, df, epoch_id):
        agg_count = df.count()
        logger.info(
            f"🔍 DEBUG Aggregation batch {epoch_id}: {agg_count} aggregation records"
        )

        if agg_count > 0:
            rows = df.collect()
            for row in rows:
                data = row.asDict()
                logger.info(
                    f"🔍 Aggregation record: Station={data.get('station_name')}, "
                    f"Total departures={data.get('total_departures')}, "
                    f"Window={data.get('window_start')} to {data.get('window_end')}"
                )
        else:
            logger.warning(f"🔍 No aggregation data in batch {epoch_id}")

    def create_station_aggregations(self, enriched_df):
        logger.info("📊 Creating station-level aggregations with tumbling windows...")

        debug_stream = enriched_df.select(
            "station_id",
            "station_name",
            "trip_id",
            "collected_at_ts",
            "planned_departure_ts",
            "line_id",
        )

        station_aggs = (
            enriched_df.groupBy(
                window(col("collected_at_ts"), self.window_duration),
                col("station_id"),
                col("station_name"),
                col("latitude"),
                col("longitude"),
            )
            .agg(
                avg("delay_minutes").alias("avg_delay"),
                spark_max("delay_minutes").alias("max_delay"),
                approx_count_distinct("trip_id", 0.01).alias("total_departures"),
                spark_sum("is_delayed").alias("delayed_departures"),
                spark_sum("is_on_time").alias("on_time_departures"),
                spark_sum("is_significant_delay").alias("significant_delays"),
                count("*").alias("total_records"),
                approx_count_distinct("line_id", 0.01).alias("unique_lines"),
            )
            .withColumn(
                "on_time_pct",
                (col("on_time_departures") / col("total_departures") * 100),
            )
            .withColumn(
                "delay_pct", (col("delayed_departures") / col("total_departures") * 100)
            )
            .withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .drop("window")
        )

        return station_aggs, debug_stream

    def _cleanup_checkpoints(self):
        checkpoint_base = Path("/tmp/spark-checkpoint")
        if checkpoint_base.exists():
            try:
                shutil.rmtree(checkpoint_base)
                logger.info("🧹 Cleaned up existing checkpoint directories")
            except Exception as e:
                logger.warning(f"Failed to clean checkpoints: {e}")
        checkpoint_base.mkdir(parents=True, exist_ok=True)

    def store_station_metrics(self, df, epoch_id):
        logger.info(f"💾 Storing station metrics for epoch {epoch_id}")

        try:
            self.redis_client.redis_client.ping()

            total_rows = df.count()
            logger.info(f"🔍 Processing {total_rows} aggregated station metrics")

            if total_rows == 0:
                logger.warning(f"⚠️ No station metrics to store for epoch {epoch_id}")
                return

            rows = df.collect()
            stored_count = 0

            for row in rows:
                try:
                    data = row.asDict()

                    if "hauptbahnhof" in data.get("station_name", "").lower():
                        logger.info(f"🚂 Hauptbahnhof metrics: {data}")

                    metrics = {
                        "station_id": data["station_id"],
                        "station_name": data["station_name"],
                        "window_start": data["window_start"].isoformat(),
                        "window_end": data["window_end"].isoformat(),
                        "avg_delay": round(data.get("avg_delay", 0), 2),
                        "max_delay": round(data.get("max_delay", 0), 2),
                        "total_departures": data.get("total_departures", 0),
                        "delayed_departures": data.get("delayed_departures", 0),
                        "on_time_departures": data.get("on_time_departures", 0),
                        "significant_delays": data.get("significant_delays", 0),
                        "on_time_pct": round(data.get("on_time_pct", 0), 1),
                        "delay_pct": round(data.get("delay_pct", 0), 1),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "total_records": data.get("total_records", 0),
                        "unique_lines": data.get("unique_lines", 0),
                        "updated_at": datetime.now().isoformat(),
                    }

                    if self.redis_client.store_station_metrics(metrics):
                        stored_count += 1

                except Exception as e:
                    logger.warning(f"Failed to store station metric: {e}")
                    continue

            logger.info(f"✅ Stored {stored_count}/{total_rows} station metrics")

        except Exception as e:
            logger.error(f"❌ Failed to store station metrics: {e}")

    def store_departure_sync(self, departure_data: dict):
        return self.redis_client.store_departure(departure_data)

    def write_to_redis_foreach_batch(self, df, epoch_id):
        batch_count = df.count()
        if batch_count == 0:
            logger.debug(f"📦 Batch {epoch_id}: No new departures to process")
            return

        logger.info(f"📦 Processing batch {epoch_id} with {batch_count} departures")

        sample_row = df.first()
        if sample_row:
            sample_dict = sample_row.asDict()
            logger.info(
                f"🕒 Sample timestamps - collected_at_ts: {sample_dict.get('collected_at_ts')}, current_time: {datetime.now()}"
            )

        try:
            self.redis_client.redis_client.ping()
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

        except Exception as e:
            logger.error(f"❌ Error processing batch {epoch_id}: {e}")

    def start_processing(self):
        try:
            logger.info(
                "🚀 Starting enhanced BVG processing pipeline with tumbling windows..."
            )

            kafka_stream = self.create_kafka_stream()
            enriched_stream = self.parse_and_filter_messages(kafka_stream)

            output_stream = enriched_stream.select(
                "trip_id",
                "station_id",
                "station_name",
                "line_id",
                "line_name",
                "transport_mode",
                "direction",
                "planned_departure",
                "actual_departure",
                "delay_minutes",
                "platform",
                "planned_platform",
                "collected_at",
                "collected_at_ts",
                "longitude",
                "latitude",
            )

            station_aggs, debug_stream = self.create_station_aggregations(
                enriched_stream
            )

            departure_query = (
                output_stream.writeStream.foreachBatch(
                    self.write_to_redis_foreach_batch
                )
                .outputMode("append")
                .option("checkpointLocation", "/tmp/spark-checkpoint/bvg-departures")
                .trigger(processingTime=settings.SPARK_PROCESSING_INTERVAL)
                .start()
            )

            debug_query = (
                debug_stream.writeStream.foreachBatch(
                    lambda df, epoch_id: logger.info(
                        f"🔍 DEBUG Raw data batch {epoch_id}: {df.count()} records available for aggregation"
                    )
                )
                .outputMode("append")
                .option("checkpointLocation", "/tmp/spark-checkpoint/debug-stream")
                .trigger(processingTime="30 seconds")
                .start()
            )

            station_query = (
                station_aggs.writeStream.foreachBatch(
                    self.debug_aggregation_foreach_batch
                )
                .outputMode("update")
                .option("checkpointLocation", "/tmp/spark-checkpoint/debug-aggregation")
                .trigger(processingTime="90 seconds")
                .start()
            )

            station_storage_query = (
                station_aggs.writeStream.foreachBatch(self.store_station_metrics)
                .outputMode("update")
                .option("checkpointLocation", "/tmp/spark-checkpoint/station-metrics")
                .trigger(processingTime="1 minute")
                .start()
            )

            logger.info("✅ Enhanced streaming queries started successfully!")
            logger.info(f"📊 Processing interval: {settings.SPARK_PROCESSING_INTERVAL}")
            logger.info(f"🔑 Redis key patterns:")
            logger.info(
                f"   - Departures: {settings.REDIS_DEPARTURES_KEY_PREFIX}station:{{station_id}}"
            )
            logger.info(
                f"   - Station metrics: metrics:station:{{station_id}}:window:{{timestamp}}"
            )
            logger.info(f"📈 Aggregation windows: {self.window_duration} (tumbling)")
            logger.info("🎯 Real-time departure data + windowed analytics available")

            return departure_query, station_query, debug_query, station_storage_query

        except Exception as e:
            logger.error(f"❌ Failed to start enhanced processing: {e}")
            raise

    def stop_processing(self, *queries):
        """Stop the streaming queries and close connections"""
        logger.info("🛑 Stopping enhanced streaming queries...")
        query_names = ["Departure", "Station", "Line"]

        for i, query in enumerate(queries):
            if query and query.isActive:
                query.stop()
                query_name = query_names[i] if i < len(query_names) else f"Query-{i}"
                logger.info(f"✅ {query_name} query stopped")

        try:
            if self.redis_client:
                self.redis_client.close_connection()
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")

        if self.spark:
            self.spark.stop()
            logger.info("✅ Enhanced Spark session closed")
