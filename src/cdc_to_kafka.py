from pyflink.datastream import StreamExecutionEnvironment, ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import sys

def create_table_environment():
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing
    env.enable_checkpointing(5000)  # 5 seconds 
    env.get_checkpoint_config().set_checkpoint_storage_dir("s3a://flink-checkpoints/cdc-to-kafka/")
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
            'connection.options' = 'authSource=admin&appName=kafka-cdc&maxPoolSize=20&minPoolSize=5',
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
            'connection.options' = 'authSource=admin&appName=kafka-cdc&maxPoolSize=20&minPoolSize=5',
            'scan.startup.mode' = 'initial'
        )
    """)

def create_kafka_sinks(table_env):

    table_env.execute_sql("""
        CREATE TABLE devices_kafka_sink (
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
            event_timestamp TIMESTAMP(3) METADATA FROM 'timestamp'
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'devices-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'debezium-json'
        )
    """)
    
    table_env.execute_sql("""
        CREATE TABLE sensor_readings_kafka_sink (
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
            event_timestamp TIMESTAMP(3) METADATA FROM 'timestamp'
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor-readings-cdc',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'debezium-json'
        )
    """)

def submit_cdc_jobs(table_env):

    # Job 1: Devices CDC to Kafka
    devices_job = table_env.execute_sql("""
        INSERT INTO devices_kafka_sink
        SELECT 
            _id,
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
            CURRENT_TIMESTAMP AS event_timestamp
        FROM devices_cdc_source
    """)
    
    # Job 2: Sensor Readings CDC to Kafka
    sensor_readings_job = table_env.execute_sql("""
        INSERT INTO sensor_readings_kafka_sink
        SELECT 
            _id,
            reading_id,
            device_id,
            sensor_value,
            unit,
            quality,
            `timestamp`,
            battery_level,
            created_at,
            updated_at,
            CURRENT_TIMESTAMP AS event_timestamp
        FROM sensor_readings_cdc_source
    """)
    
    print("Done")

    return [devices_job, sensor_readings_job]

def main():
    
    try:
        # Create table environment
        table_env = create_table_environment()
        
        create_mongodb_cdc_sources(table_env)
        
        create_kafka_sinks(table_env)
        
        submit_cdc_jobs(table_env)
        
    except Exception as e:
        print(f"Error in IoT CDC to Kafka job: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
