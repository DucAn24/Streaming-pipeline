#!/usr/bin/env python3
from pyflink.datastream import StreamExecutionEnvironment,ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import sys

def create_table_environment():
    # Create stream execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Enable checkpointing
    env.enable_checkpointing(5000)  # 5 seconds 
    env.get_checkpoint_config().set_checkpoint_storage_dir("s3a://flink-checkpoints/cdc-to-es/")
    env.get_checkpoint_config().set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    # Create table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)

    return table_env

def create_mongodb_cdc_sources(table_env):


    table_env.execute_sql("""
        CREATE TABLE devices_cdc_source (
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
            'connector' = 'mongodb-cdc',
            'hosts' = 'mongodb:27017',
            'username' = 'admin',
            'password' = 'password',
            'database' = 'demo_db',
            'collection' = 'devices',
            'connection.options' = 'authSource=admin&appName=es-cdc&maxPoolSize=20&minPoolSize=5',
            'scan.startup.mode' = 'initial'
        )
    """)
    
    table_env.execute_sql("""
        CREATE TABLE sensor_readings_cdc_source (
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
            'connector' = 'mongodb-cdc',
            'hosts' = 'mongodb:27017',
            'username' = 'admin',
            'password' = 'password',
            'database' = 'demo_db',
            'collection' = 'sensor_readings',
            'connection.options' = 'authSource=admin&appName=es-cdc&maxPoolSize=20&minPoolSize=5',
            'scan.startup.mode' = 'initial'
        )
    """)

def create_joined_views(table_env):
    
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
            COALESCE(s.`timestamp`, s.created_at, CURRENT_TIMESTAMP) as event_timestamp
        FROM devices_cdc_source d
        LEFT JOIN sensor_readings_cdc_source s
        ON d.device_id = s.device_id
    """)


def create_elasticsearch_sinks(table_env):
    
    # Elasticsearch sink for devices
    table_env.execute_sql("""
        CREATE TABLE elasticsearch_devices (
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
            PRIMARY KEY (mongo_id) NOT ENFORCED
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://es-container:9200',
            'index' = 'devices',
            'sink.bulk-flush.max-actions' = '1',
            'sink.bulk-flush.max-size' = '1mb',
            'sink.bulk-flush.interval' = '1s',
            'format' = 'json'
        )
    """)
    
    # Elasticsearch sink for sensor readings
    table_env.execute_sql("""
        CREATE TABLE elasticsearch_sensor_readings (
            mongo_id STRING,
            reading_id STRING,
            device_id STRING,
            sensor_value DOUBLE,
            unit STRING,
            quality STRING,
            `timestamp` TIMESTAMP(3),
            battery_level INT,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            PRIMARY KEY (mongo_id) NOT ENFORCED
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://es-container:9200',
            'index' = 'sensor_readings',
            'sink.bulk-flush.max-actions' = '1',
            'sink.bulk-flush.max-size' = '1mb',
            'sink.bulk-flush.interval' = '1s',
            'format' = 'json'
        )
    """)
    
    # Elasticsearch sink for joined devices + readings
    table_env.execute_sql("""
        CREATE TABLE elasticsearch_devices_readings (
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
            PRIMARY KEY (reading_mongo_id) NOT ENFORCED
        ) WITH (
            'connector' = 'elasticsearch-7',
            'hosts' = 'http://es-container:9200',
            'index' = 'devices_readings',
            'sink.bulk-flush.max-actions' = '1',
            'sink.bulk-flush.max-size' = '1mb',
            'sink.bulk-flush.interval' = '1s',
            'format' = 'json'
        )
    """)

def submit_processing_jobs(table_env):
    
    devices_job = table_env.execute_sql("""
        INSERT INTO elasticsearch_devices
        SELECT 
            _id AS mongo_id,
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
            updated_at
        FROM devices_cdc_source
    """)
    
    sensor_readings_job = table_env.execute_sql("""
        INSERT INTO elasticsearch_sensor_readings
        SELECT 
            _id AS mongo_id,
            reading_id,
            device_id,
            sensor_value,
            unit,
            quality,
            `timestamp`,
            battery_level,
            created_at,
            updated_at
        FROM sensor_readings_cdc_source
    """)
    
    # Process joined devices + readings data
    joined_job = table_env.execute_sql("""
        INSERT INTO elasticsearch_devices_readings
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
            device_status,
            device_battery,
            reading_id,
            sensor_value,
            unit,
            quality,
            severity,
            reading_timestamp,
            reading_battery,
            event_timestamp
        FROM devices_readings_joined
        WHERE reading_id IS NOT NULL
    """)
    
    print("Done ")

    return [devices_job, sensor_readings_job, joined_job]

def main():
    try:
        # Create table environment
        table_env = create_table_environment()
        
        # Create MongoDB CDC source tables
        create_mongodb_cdc_sources(table_env)
        
        # Create joined views 
        create_joined_views(table_env)
        
        # Create Elasticsearch sinks 
        create_elasticsearch_sinks(table_env) 
        
        # Submit processing jobs
        submit_processing_jobs(table_env)
        
    except Exception as e:
        print(f"Error in MongoDB to Elasticsearch processing job: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
