#!/usr/bin/env python3
from pyflink.datastream import StreamExecutionEnvironment, ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import sys

def create_table_environment():
    # Create stream execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Configure checkpoint storage to MinIO (S3A)
    env.enable_checkpointing(5000)  # 5 seconds 
    env.get_checkpoint_config().set_checkpoint_storage_dir("s3a://flink-checkpoints/kafka-to-delta/")
    env.get_checkpoint_config().set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    
    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Configure event time and watermark
    table_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "30s")
    table_env.get_config().get_configuration().set_string("table.exec.state.ttl", "2h")

    # Configure Delta catalog
    table_env.execute_sql("""
        CREATE CATALOG delta_catalog WITH (
            'type' = 'delta-catalog'
        )
    """)
    
    # Use the Delta catalog
    table_env.execute_sql("USE CATALOG delta_catalog")
    
    return table_env

def create_kafka_sources(table_env):
    """Create Kafka source tables for IoT data"""

    table_env.execute_sql("""
        CREATE TABLE devices_kafka_source_raw (
            `after` ROW<
                `_id` STRING,
                `device_id` STRING,
                `device_name` STRING,
                `device_type` STRING,
                `location` STRING,
                `building` STRING,
                `floor_level` INT,
                `area_type` STRING,
                `temperature_threshold` DOUBLE,
                `installed_date` STRING,
                `status` STRING,
                `battery_level` INT,
                `created_at` STRING,
                `updated_at` STRING
            >,
            `op` STRING,
            `ts_ms` BIGINT,
            event_time AS CASE 
                WHEN ts_ms IS NOT NULL THEN TO_TIMESTAMP_LTZ(ts_ms, 3)
                ELSE CURRENT_TIMESTAMP
            END,
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'devices-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-devices-delta-consumer',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'scan.startup.mode' = 'group-offsets'
        )
    """)
    
    table_env.execute_sql("""
        CREATE VIEW devices_kafka_source AS
        SELECT 
            `after`.`_id` as mongo_id,
            `after`.`device_id` as device_id,
            `after`.`device_name` as device_name,
            `after`.`device_type` as device_type,
            `after`.`location` as location,
            `after`.`building` as building,
            `after`.`floor_level` as floor_level,
            `after`.`area_type` as area_type,
            `after`.`temperature_threshold` as temperature_threshold,
            CASE 
                WHEN `after`.`installed_date` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`installed_date`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as installed_date,
            `after`.`status` as status,
            `after`.`battery_level` as battery_level,
            CASE 
                WHEN `after`.`created_at` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`created_at`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as created_at,
            CASE 
                WHEN `after`.`updated_at` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`updated_at`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as updated_at,
            event_time as event_timestamp
        FROM devices_kafka_source_raw
        WHERE `after` IS NOT NULL AND `op` = 'c'
    """)
    
    table_env.execute_sql("""
        CREATE TABLE sensor_readings_kafka_source_raw (
            `after` ROW<
                `_id` STRING,
                `reading_id` STRING,
                `device_id` STRING,
                `sensor_value` DOUBLE,
                `unit` STRING,
                `quality` STRING,
                `timestamp` STRING,
                `battery_level` INT,
                `created_at` STRING,
                `updated_at` STRING
            >,
            `op` STRING,
            `ts_ms` BIGINT,
            event_time AS CASE 
                WHEN ts_ms IS NOT NULL THEN TO_TIMESTAMP_LTZ(ts_ms, 3)
                ELSE CURRENT_TIMESTAMP
            END,
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor-readings-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-sensor-readings-delta-consumer',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'scan.startup.mode' = 'group-offsets'
        )
    """)
    
    table_env.execute_sql("""
        CREATE VIEW sensor_readings_kafka_source AS
        SELECT 
            `after`.`_id` as mongo_id,
            `after`.`reading_id` as reading_id,
            `after`.`device_id` as device_id,
            `after`.`sensor_value` as sensor_value,
            `after`.`unit` as unit,
            `after`.`quality` as quality,
            CASE 
                WHEN `after`.`timestamp` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`timestamp`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as `timestamp`,
            `after`.`battery_level` as battery_level,
            CASE 
                WHEN `after`.`created_at` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`created_at`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as created_at,
            CASE 
                WHEN `after`.`updated_at` IS NOT NULL 
                THEN TO_TIMESTAMP(`after`.`updated_at`, 'yyyy-MM-dd HH:mm:ss.SSS')
                ELSE CURRENT_TIMESTAMP
            END as updated_at,
            event_time as event_timestamp
        FROM sensor_readings_kafka_source_raw
        WHERE `after` IS NOT NULL AND `op` = 'c'
    """)
    

    # devices + sensor_readings
    table_env.execute_sql("""
        CREATE VIEW devices_readings_joined AS
        SELECT 
            d.mongo_id as device_mongo_id,
            s.mongo_id as reading_mongo_id,
            d.device_id,
            d.device_name,
            d.device_type,
            d.location,
            d.building,
            d.floor_level,
            d.area_type,
            d.temperature_threshold,
            d.installed_date,
            d.status as device_status,
            d.battery_level as device_battery,
            s.reading_id,
            s.sensor_value,
            s.unit,
            s.quality,
            CASE 
                WHEN s.sensor_value > d.temperature_threshold + 5 THEN 'CRITICAL'
                WHEN s.sensor_value > d.temperature_threshold THEN 'WARNING'
                ELSE 'NORMAL'
            END as severity,
            s.`timestamp` as reading_timestamp,
            s.battery_level as reading_battery,
            COALESCE(s.event_timestamp, d.event_timestamp) as event_timestamp
        FROM devices_kafka_source d
        LEFT JOIN sensor_readings_kafka_source s
        ON d.device_id = s.device_id
    """)
    
    table_env.execute_sql("""
        CREATE VIEW temperature_readings_per_minute AS
        SELECT 
            TUMBLE_START(s.event_timestamp, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(s.event_timestamp, INTERVAL '1' MINUTE) as window_end,
                          
            s.device_id,
            d.device_name,
            d.location,
            d.building,
                          
            COUNT(*) as reading_count,
            AVG(s.sensor_value) as avg_temperature,
            MAX(s.sensor_value) as max_temperature,
            MIN(s.sensor_value) as min_temperature,
            COUNT(CASE WHEN s.quality = 'warning' THEN 1 END) as warning_count,
            COUNT(CASE WHEN s.quality = 'critical' THEN 1 END) as critical_count
                          
        FROM sensor_readings_kafka_source s
        INNER JOIN devices_kafka_source d ON s.device_id = d.device_id
        WHERE s.reading_id IS NOT NULL 
        AND d.device_type = 'temperature'
        AND s.unit = '°C'
        GROUP BY 
            TUMBLE(s.event_timestamp, INTERVAL '1' MINUTE),
            s.device_id, d.device_name, d.location, d.building
    """)

def create_delta_lake_sinks(table_env):
    
    # Raw devices CDC sink
    table_env.execute_sql("""
        CREATE TABLE delta_devices_raw (
            mongo_id STRING,
            device_id STRING,
            device_name STRING,
            device_type STRING,
            location STRING,
            building STRING,
            floor_level INT,
            area_type STRING,
            temperature_threshold DOUBLE,
            installed_date TIMESTAMP(3),
            status STRING,
            battery_level INT,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            event_timestamp TIMESTAMP(3),
            cdc_ts BIGINT,
            partition_date STRING
        ) PARTITIONED BY (partition_date) WITH (
            'connector' = 'delta',
            'table-path' = 's3a://delta-lake/devices_raw'
        )
    """)

    # Raw sensor readings CDC sink
    table_env.execute_sql("""
        CREATE TABLE delta_sensor_readings_raw (
            mongo_id STRING,
            reading_id STRING,
            device_id STRING,
            sensor_value DOUBLE,
            unit STRING,
            quality STRING,
            reading_timestamp TIMESTAMP(3),
            battery_level INT,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            event_timestamp TIMESTAMP(3),
            cdc_ts BIGINT,
            partition_date STRING
        ) PARTITIONED BY (partition_date) WITH (
            'connector' = 'delta',
            'table-path' = 's3a://delta-lake/sensor_readings_raw'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE delta_devices_readings (
            device_mongo_id STRING,
            reading_mongo_id STRING,
            device_id STRING,
            device_name STRING,
            device_type STRING,
            location STRING,
            building STRING,
            floor_level INT,
            area_type STRING,
            temperature_threshold DOUBLE,
            installed_date TIMESTAMP(3),
            device_status STRING,
            device_battery INT,
            reading_id STRING,
            sensor_value DOUBLE,
            unit STRING,
            quality STRING,
            severity STRING,
            reading_timestamp TIMESTAMP(3),
            reading_battery INT,
            event_timestamp TIMESTAMP(3),
            cdc_ts BIGINT,
            partition_date STRING
        ) PARTITIONED BY (partition_date) WITH (
            'connector' = 'delta',
            'table-path' = 's3a://delta-lake/devices_readings'
        )
    """)
    
    table_env.execute_sql("""
        CREATE TABLE delta_temperature_readings_minute (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            device_id STRING,
            device_name STRING,
            location STRING,
            building STRING,
            reading_count BIGINT,
            avg_temperature DOUBLE,
            max_temperature DOUBLE,
            min_temperature DOUBLE,
            warning_count BIGINT,
            critical_count BIGINT,
            cdc_ts BIGINT,
            partition_date STRING
        ) PARTITIONED BY (partition_date) WITH (
            'connector' = 'delta',
            'table-path' = 's3a://delta-lake/temperature_readings_minute'
        )
    """)

def submit_processing_jobs(table_env):
    # Stream raw devices data into Delta (MinIO)
    table_env.execute_sql("""
        INSERT INTO delta_devices_raw
        SELECT 
            mongo_id,
            device_id,
            device_name,
            device_type,
            location,
            building,
            floor_level,
            area_type,
            temperature_threshold,
            installed_date,
            status,
            battery_level,
            created_at,
            updated_at,
            event_timestamp,
            CAST(EXTRACT(EPOCH FROM event_timestamp) * 1000 AS BIGINT) as cdc_ts,
            COALESCE(DATE_FORMAT(event_timestamp, 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM devices_kafka_source
    """)

    # Stream raw sensor readings into Delta (MinIO)
    table_env.execute_sql("""
        INSERT INTO delta_sensor_readings_raw
        SELECT 
            mongo_id,
            reading_id,
            device_id,
            sensor_value,
            unit,
            quality,
            `timestamp` as reading_timestamp,
            battery_level,
            created_at,
            updated_at,
            event_timestamp,
            CAST(EXTRACT(EPOCH FROM event_timestamp) * 1000 AS BIGINT) as cdc_ts,
            COALESCE(DATE_FORMAT(event_timestamp, 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM sensor_readings_kafka_source
    """)

    table_env.execute_sql("""
        INSERT INTO delta_devices_readings
        SELECT 
            device_mongo_id,
            reading_mongo_id,
            device_id,
            device_name,
            device_type,
            location,
            building,
            floor_level,
            area_type,
            temperature_threshold,
            installed_date,
            device_status,
            device_battery,
            reading_id,
            sensor_value,   
            unit,
            quality,
            severity,
            reading_timestamp,
            reading_battery,
            event_timestamp,
            CAST(EXTRACT(EPOCH FROM event_timestamp) * 1000 AS BIGINT) as cdc_ts,
            COALESCE(DATE_FORMAT(event_timestamp, 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM devices_readings_joined
        WHERE reading_id IS NOT NULL
    """)

    table_env.execute_sql("""
        INSERT INTO delta_temperature_readings_minute
        SELECT 
            window_start,
            window_end,
            device_id,
            device_name,
            location,
            building,
            reading_count,
            avg_temperature,
            max_temperature,
            min_temperature,
            warning_count,
            critical_count,
            CAST(EXTRACT(EPOCH FROM window_start) * 1000 AS BIGINT) as cdc_ts,
            COALESCE(DATE_FORMAT(window_start, 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM temperature_readings_per_minute
    """)


def main():
    try:
        # Create table environment
        table_env = create_table_environment()
        
        # Create Kafka source tables for IoT data
        create_kafka_sources(table_env)
        
        # Create Delta Lake sinks for IoT data (append-only)
        create_delta_lake_sinks(table_env) 
        
        # Submit IoT processing jobs
        submit_processing_jobs(table_env)
        
    except Exception as e:
        print(f"Error in IoT processing job: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()