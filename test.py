#!/usr/bin/env python3
import pymongo
import time
import random
from datetime import datetime, timezone
import signal
import sys

class SimpleTestGenerator:
    def __init__(self):
        # MongoDB connection
        self.client = pymongo.MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")
        self.db = self.client.demo_db
        self.devices_collection = self.db.devices
        self.readings_collection = self.db.sensor_readings
        
        # Control flag
        self.running = True
        signal.signal(signal.SIGINT, self._stop)
        
        # Predefined device IDs từ mongo-init.sh
        self.device_ids = [
            'MP1-N5-D1_CAM-01',
            'TH-VD4-DA2_CAM-02', 
            'MP3-NE8-DE6_CAM-01',
            'LH-VVK-HV_CAM-04'
        ]
        
        print(f"🚀 Simple Test Generator initialized with {len(self.device_ids)} devices")
    
    def _stop(self, signum, frame):
        print(f"\n⏹️  Stopping generator...")
        self.running = False
    
    def create_new_device(self):
        """Create a new device using predefined device IDs"""
        current_time = datetime.now(timezone.utc)
        
        # Use predefined device ID
        device_id = random.choice(self.device_ids)
        
        # Create new device data
        device_data = {
            'device_id': device_id,
            'device_name': f"Test Camera {random.randint(100, 999)}",
            'device_type': random.choice(['camera', 'sensor', 'gateway']),
            'location': random.choice(['Building A', 'Building B', 'Building C']),
            'building': random.choice(['North Tower', 'South Tower', 'Main Building']),
            'floor_level': random.randint(1, 10),
            'area_type': random.choice(['office', 'lobby', 'parking', 'corridor']),
            'temperature_threshold': round(random.uniform(25.0, 35.0), 1),
            'installed_date': current_time,
            'status': 'active',
            'battery_level': random.randint(80, 100),
            'created_at': current_time,
            'updated_at': current_time
        }
        
        try:
            result = self.devices_collection.insert_one(device_data)
            
            if result.inserted_id:
                print(f"✅ Created new device {device_id}: type={device_data['device_type']}, location={device_data['location']}")
                return True
            else:
                print(f"⚠️  Failed to create device {device_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating device {device_id}: {e}")
            return False
    
    def insert_random_reading(self):
        """Insert một sensor reading random"""
        device_id = random.choice(self.device_ids)
        current_time = datetime.now(timezone.utc)
        
        # Generate reading
        reading_data = {
            'reading_id': f"{device_id}_{int(current_time.timestamp())}",
            'device_id': device_id,
            'sensor_value': round(random.uniform(20.0, 45.0), 1),
            'unit': '°C',
            'quality': random.choice(['good', 'warning', 'critical']),
            'timestamp': current_time,
            'battery_level': random.randint(20, 100),
            'created_at': current_time,
            'updated_at': current_time
        }
        
        try:
            result = self.readings_collection.insert_one(reading_data)
            
            if result.inserted_id:
                print(f"📊 New reading {device_id}: {reading_data['sensor_value']}°C ({reading_data['quality']})")
                return True
            else:
                print(f"⚠️  Failed to insert reading for {device_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error inserting reading for {device_id}: {e}")
            return False
    
    def run(self):
        """Chạy test generator mỗi giây - chỉ CREATE operations"""
        print("🔄 Starting simple test generator (CREATE operations only)")
        print("Press Ctrl+C to stop")
        
        counter = 0
        
        while self.running:
            try:
                counter += 1
                
                # Alternating between device creation and sensor readings (both are CREATE operations)
                if counter % 2 == 0:
                    success = self.create_new_device()
                    event_type = "DEVICE_CREATE"
                else:
                    success = self.insert_random_reading()
                    event_type = "SENSOR_READING"
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Event #{counter} ({event_type}) - {'SUCCESS' if success else 'FAILED'}")

                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                time.sleep(1)
        
        print(f"\n📊 Test completed. Generated {counter} events total.")
        self.client.close()

def main():
    generator = SimpleTestGenerator()
    
    try:
        generator.run()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()