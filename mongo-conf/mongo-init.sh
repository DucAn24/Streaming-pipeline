#!/bin/bash

echo "Starting MongoDB initialization..."

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to start..."
sleep 10

# Initialize replica set (without keyfile first)
echo "Initializing replica set..."
mongosh --eval "
try {
    rs.initiate({
        _id: 'rs0',
        members: [
            { _id: 0, host: 'localhost:27017' }
        ]
    });
    print('Replica set initialized successfully');
} catch(e) {
    print('Replica set might already be initialized: ' + e);
}
"

sleep 5

# Create admin user
echo "Creating admin user..."
mongosh --eval "
db = db.getSiblingDB('admin');
try {
    db.createUser({
        user: 'admin',
        pwd: 'password',
        roles: [
            { role: 'userAdminAnyDatabase', db: 'admin' },
            { role: 'readWriteAnyDatabase', db: 'admin' },
            { role: 'dbAdminAnyDatabase', db: 'admin' },
            { role: 'clusterAdmin', db: 'admin' }
        ]
    });
    print('Admin user created successfully');
} catch(e) {
    print('Admin user might already exist: ' + e);
}
"

# Create sample data
echo "Creating sample data in demo_db..."
mongosh --eval "
db = db.getSiblingDB('demo_db');

// Check if data already exists to prevent duplicates
if (db.devices.countDocuments() > 0) {
    print('IoT sample data already exists, skipping creation...');
} else {
    print('Creating devices collection for IoT demo...');
    
    var baseTime = new Date('2025-01-01T10:00:00Z');
    
    db.devices.insertMany([
        {
            device_id: 'MP1-N5-D1_CAM-01',
            device_name: 'Temperature Sensor Room 1',
            device_type: 'temperature',
            location: 'Server Room 1',
            building: 'Building A',
            floor_level: 1,
            area_type: 'technical',
            temperature_threshold: 30.0,
            installed_date: new Date(baseTime.getTime() - 30*24*60*60*1000),
            status: 'active',
            battery_level: 85,
            created_at: new Date(baseTime.getTime() - 30*24*60*60*1000),
            updated_at: new Date(baseTime.getTime() - 30*24*60*60*1000)
        },
        {
            device_id: 'TH-VD4-DA2_CAM-02',
            device_name: 'Humidity Sensor Room 1',
            device_type: 'humidity',
            location: 'Server Room 1',
            building: 'Building A',
            floor_level: 1,
            area_type: 'technical',
            temperature_threshold: 30.0,
            installed_date: new Date(baseTime.getTime() - 30*24*60*60*1000),
            status: 'active',
            battery_level: 78,
            created_at: new Date(baseTime.getTime() - 30*24*60*60*1000),
            updated_at: new Date(baseTime.getTime() - 30*24*60*60*1000)
        },
        {
            device_id: 'MP3-NE8-DE6_CAM-01',
            device_name: 'Temperature Sensor Warehouse',
            device_type: 'temperature',
            location: 'Warehouse A',
            building: 'Building B',
            floor_level: 0,
            area_type: 'storage',
            temperature_threshold: 35.0,
            installed_date: new Date(baseTime.getTime() - 15*24*60*60*1000),
            status: 'active',
            battery_level: 92,
            created_at: new Date(baseTime.getTime() - 15*24*60*60*1000),
            updated_at: new Date(baseTime.getTime() - 15*24*60*60*1000)
        },
        {
            device_id: 'LH-VVK-HV_CAM-04',
            device_name: 'Temperature Sensor Office',
            device_type: 'temperature',
            location: 'Office Floor 3',
            building: 'Building A',
            floor_level: 3,
            area_type: 'office',
            temperature_threshold: 28.0,
            installed_date: new Date(baseTime.getTime() - 60*24*60*60*1000),
            status: 'active',
            battery_level: 23,
            created_at: new Date(baseTime.getTime() - 60*24*60*60*1000),
            updated_at: new Date(baseTime.getTime() - 60*24*60*60*1000)
        }
    ]);

    print('Creating sensor_readings collection for IoT demo...');
    
    db.sensor_readings.insertMany([
        // TEMP_001: Server Room - Normal reading
        {
            reading_id: 'TEMP_001_NORMAL',
            device_id: 'MP1-N5-D1_CAM-01',
            sensor_value: 25.5,
            unit: '°C',
            quality: 'good',
            timestamp: baseTime,
            battery_level: 85,
            created_at: baseTime,
            updated_at: baseTime
        },
        
        // TEMP_001: Server Room - WARNING (over 30°C threshold)
        {
            reading_id: 'TEMP_001_WARNING',
            device_id: 'MP1-N5-D1_CAM-01',
            sensor_value: 32.5,
            unit: '°C',
            quality: 'warning',
            timestamp: new Date(baseTime.getTime() + 5*60*1000),
            battery_level: 85,
            created_at: new Date(baseTime.getTime() + 5*60*1000),
            updated_at: new Date(baseTime.getTime() + 5*60*1000)
        },
        
        // TEMP_001: Server Room - CRITICAL (over 35°C)
        {
            reading_id: 'TEMP_001_CRITICAL',
            device_id: 'MP1-N5-D1_CAM-01',
            sensor_value: 37.8,
            unit: '°C',
            quality: 'critical',
            timestamp: new Date(baseTime.getTime() + 10*60*1000),
            battery_level: 85,
            created_at: new Date(baseTime.getTime() + 10*60*1000),
            updated_at: new Date(baseTime.getTime() + 10*60*1000)
        },
        
        // TEMP_002: Warehouse - Normal reading
        {
            reading_id: 'TEMP_002_NORMAL',
            device_id: 'MP3-NE8-DE6_CAM-01',
            sensor_value: 20.3,
            unit: '°C',
            quality: 'good',
            timestamp: baseTime,
            battery_level: 92,
            created_at: baseTime,
            updated_at: baseTime
        },
        
        // TEMP_002: Warehouse - WARNING (over 35°C threshold)
        {
            reading_id: 'TEMP_002_WARNING',
            device_id: 'MP3-NE8-DE6_CAM-01',
            sensor_value: 38.1,
            unit: '°C',
            quality: 'warning',
            timestamp: new Date(baseTime.getTime() + 15*60*1000),
            battery_level: 92,
            created_at: new Date(baseTime.getTime() + 15*60*1000),
            updated_at: new Date(baseTime.getTime() + 15*60*1000)
        },
        
        // TEMP_003: Office - Normal reading
        {
            reading_id: 'TEMP_003_NORMAL',
            device_id: 'LH-VVK-HV_CAM-04',
            sensor_value: 24.7,
            unit: '°C',
            quality: 'good',
            timestamp: baseTime,
            battery_level: 23,
            created_at: baseTime,
            updated_at: baseTime
        },
        
        // TEMP_003: Office - WARNING (over 28°C threshold)
        {
            reading_id: 'TEMP_003_WARNING',
            device_id: 'LH-VVK-HV_CAM-04',
            sensor_value: 30.2,
            unit: '°C',
            quality: 'warning',
            timestamp: new Date(baseTime.getTime() + 20*60*1000),
            battery_level: 23,
            created_at: new Date(baseTime.getTime() + 20*60*1000),
            updated_at: new Date(baseTime.getTime() + 20*60*1000)
        },
        
        // HUM_001: Humidity - Normal reading
        {
            reading_id: 'HUM_001_NORMAL',
            device_id: 'TH-VD4-DA2_CAM-02',
            sensor_value: 45.8,
            unit: '%',
            quality: 'good',
            timestamp: baseTime,
            battery_level: 78,
            created_at: baseTime,
            updated_at: baseTime
        }
    ]);

    print('IoT sample data created successfully');
}
"

# Enable change streams for CDC
echo "Enabling change streams for simplified IoT collections..."
mongosh --eval "
db = db.getSiblingDB('demo_db');
try {
    db.runCommand({
        collMod: 'devices',
        changeStreamPreAndPostImages: { enabled: true }
    });
    print('Change streams enabled for devices collection');
} catch(e) {
    print('Error enabling change streams for devices: ' + e);
}

try {
    db.runCommand({
        collMod: 'sensor_readings',
        changeStreamPreAndPostImages: { enabled: true }
    });
    print('Change streams enabled for sensor_readings collection');
} catch(e) {
    print('Error enabling change streams for sensor_readings: ' + e);
}

print('MongoDB initialization completed successfully');
"

echo "MongoDB initialization script completed successfully!"