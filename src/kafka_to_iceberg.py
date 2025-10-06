#!/usr/bin/env python3
from pyflink.datastream import StreamExecutionEnvironment, ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import sys

def create_table_environment():
    """Create table environment with Iceberg catalog support"""
    
    # Create stream execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Configure checkpoint storage to MinIO (S3A)
    env.enable_checkpointing(5000)  # 5 seconds 
    env.get_checkpoint_config().set_checkpoint_storage_dir("s3a://flink-checkpoints/kafka-to-iceberg/")
    env.get_checkpoint_config().set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    
    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Configure event time and watermark
    table_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "30s")
    table_env.get_config().get_configuration().set_string("table.exec.state.ttl", "2h")
    
    # Configure Iceberg catalog - Using Hive Metastore
    table_env.execute_sql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hive',
            'uri' = 'thrift://hive-metastore:9083',
            'warehouse' = 's3a://iceberg-warehouse/',
            'hive-conf-dir' = '/opt/flink/conf'
        )
    """)
    
    return table_env

def create_kafka_sources(table_env):
    """Create Kafka source tables for IoT CDC data with simplified debezium-json format"""
    
    # Devices source with debezium-json format 
    table_env.execute_sql("""
        CREATE TABLE devices_kafka_source (
            _id STRING,
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
            PRIMARY KEY (_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'devices-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-devices-iceberg-consumer',
            'format' = 'debezium-json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    # Sensor readings source with debezium-json format 
    table_env.execute_sql("""
        CREATE TABLE sensor_readings_kafka_source (
            _id STRING,
            reading_id STRING,
            device_id STRING,
            sensor_value DOUBLE,
            unit STRING,
            quality STRING,
            `timestamp` TIMESTAMP(3),
            battery_level INT,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            PRIMARY KEY (_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor-readings-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-sensor-readings-iceberg-consumer',
            'format' = 'debezium-json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    # Enriched joined view 
    table_env.execute_sql("""
        CREATE VIEW devices_readings_joined AS
        SELECT 
            d._id as device_mongo_id,
            s._id as reading_mongo_id,
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
            COALESCE(s.`timestamp`, s.updated_at, s.created_at, CURRENT_TIMESTAMP) as event_timestamp
        FROM devices_kafka_source d
        LEFT JOIN sensor_readings_kafka_source s
        ON d.device_id = s.device_id
    """)

def create_iceberg_sinks(table_env):
    """Create Iceberg sink tables with upsert support"""
    
    # Ensure Iceberg database exists
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.iot_db")

    # Devices table with primary key for upsert
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.iot_db.iceberg_devices (
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
            partition_date STRING,
            PRIMARY KEY (device_id, partition_date) NOT ENFORCED
        ) PARTITIONED BY (partition_date) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)
    
    # Sensor readings table with primary key for upsert
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.iot_db.iceberg_sensor_readings (
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
            partition_date STRING,
            PRIMARY KEY (reading_id, partition_date) NOT ENFORCED
        ) PARTITIONED BY (partition_date) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)
    
    # Enriched devices + readings table
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.iot_db.iceberg_devices_readings_enriched (
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
            partition_date STRING,
            PRIMARY KEY (reading_id, partition_date) NOT ENFORCED
        ) PARTITIONED BY (partition_date) WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)

def submit_processing_jobs(table_env):
    """Submit upsert jobs to process CDC data"""
    
    print("Starting devices upsert job...")
    devices_job = table_env.execute_sql("""
        INSERT INTO iceberg_catalog.iot_db.iceberg_devices
        SELECT 
            _id as mongo_id,
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
            COALESCE(DATE_FORMAT(COALESCE(updated_at, created_at, CURRENT_TIMESTAMP), 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM devices_kafka_source
    """)
    
    print("Starting sensor readings upsert job...")
    readings_job = table_env.execute_sql("""
        INSERT INTO iceberg_catalog.iot_db.iceberg_sensor_readings
        SELECT 
            _id as mongo_id,
            reading_id,
            device_id,
            sensor_value,
            unit,
            quality,
            `timestamp` as reading_timestamp,
            battery_level,
            created_at,
            updated_at,
            COALESCE(DATE_FORMAT(COALESCE(`timestamp`, updated_at, created_at, CURRENT_TIMESTAMP), 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM sensor_readings_kafka_source
        WHERE reading_id IS NOT NULL
    """)
    
    print("Starting enriched data upsert job...")
    enriched_job = table_env.execute_sql("""
        INSERT INTO iceberg_catalog.iot_db.iceberg_devices_readings_enriched
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
            COALESCE(DATE_FORMAT(event_timestamp, 'yyyy-MM-dd'), '2025-01-01') as partition_date
        FROM devices_readings_joined
        WHERE reading_id IS NOT NULL
    """)
    
    return [devices_job, readings_job, enriched_job]

def main():
    try:
        print("Creating Iceberg table environment...")
        table_env = create_table_environment()
        
        print("Creating Kafka CDC sources...")
        create_kafka_sources(table_env)
        
        print("Creating Iceberg sinks with upsert support...")
        create_iceberg_sinks(table_env) 
        
        print("Submitting CDC upsert processing jobs...")
        jobs = submit_processing_jobs(table_env)
        
        print("Jobs submitted successfully. Processing CDC data with automatic upserts...")
        print(f"Total jobs running: {len(jobs)}")
        print("Iceberg will automatically handle CREATE/UPDATE/DELETE operations!")
        
    except Exception as e:
        print(f"Error in IoT Iceberg CDC processing job: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
