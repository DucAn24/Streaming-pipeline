# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build --no-cache

# Start all services
up:
	@echo "Starting all services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 10
	@make setup-minio
	@make setup-kafka-topics
	@make setup-mongodb

# Setup MongoDB with error handling
setup-mongodb:
	@echo "Setting up MongoDB..."
	@echo "Waiting for MongoDB to be ready..."
	@echo "Checking MongoDB readiness..."
	@timeout 60 bash -c 'until docker exec mongodb mongosh --eval "db.adminCommand({ping:1})" &>/dev/null; do echo "Waiting for MongoDB..."; sleep 3; done' || (echo "MongoDB timeout" && exit 1)
	@echo "Setting execute permissions on mongo-init.sh..."
	@chmod +x mongo-conf/mongo-init.sh
	@echo "Running MongoDB initialization script..."
	@docker exec mongodb bash /docker-entrypoint-initdb.d/mongo-init.sh
	@echo "MongoDB setup completed successfully!"

setup-kafka-topics:
	@echo "Checking Kafka readiness..."
	@docker exec kafka kafka-topics --create --topic devices-cdc --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists || true
	@docker exec kafka kafka-topics --create --topic sensor-readings-cdc --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists || true
	@docker exec kafka kafka-topics --create --topic api-license-plate --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists || true
	@echo "Kafka topics created successfully!"

# Stop all services
down:
	@echo "Stopping all services..."
	docker-compose down -v

# Clean up everything
clean:
	@echo "Cleaning up..."
	docker-compose down -v --remove-orphans
	docker system prune -f
	@echo "Cleanup completed!"

# Setup MinIO buckets
setup-minio:
	@echo "Setting up MinIO buckets..."
	@docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin || true
	@docker exec minio mc mb myminio/flink-checkpoints --ignore-existing || true
	@docker exec minio mc mb myminio/delta-lake --ignore-existing || true
	@docker exec minio mc mb myminio/iceberg-warehouse --ignore-existing || true
	@echo "MinIO buckets created successfully!"

# List Kafka topics
kafka-topics:
	docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

cdc-kafka:
	@if [ ! -f src/cdc_to_kafka.py ]; then echo "Error: src/cdc_to_kafka.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/src/cdc_to_kafka.py

delta:
	@if [ ! -f src/kafka_to_delta.py ]; then echo "Error: src/kafka_to_delta.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/src/kafka_to_delta.py

ice:
	@if [ ! -f src/kafka_to_iceberg.py ]; then echo "Error: src/kafka_to_iceberg.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/src/kafka_to_iceberg.py

es:
	@if [ ! -f src/kafka_to_es.py ]; then echo "Error: src/kafka_to_es.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/src/kafka_to_es.py

cdc-es:
	@if [ ! -f src/cdc_to_es.py ]; then echo "Error: src/cdc_to_es.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/src/cdc_to_es.py

api:
	@if [ ! -f api-source/api.py ]; then echo "Error: api-source/api.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/api-source/api.py

udf:
	@if [ ! -f api-source/udf.py ]; then echo "Error: api-source/udf.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/api-source/udf.py

join-es:
	@if [ ! -f api-source/join-es.py ]; then echo "Error: api-source/join-es.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/api-source/join-es.py

join-delta:
	@if [ ! -f api-source/join-delta.py ]; then echo "Error: api-source/join-delta.py not found."; exit 1; fi
	docker exec -it jobmanager /opt/flink/bin/flink run -py /opt/flink/api-source/join-delta.py

# Connect to MongoDB shell
shell:
	docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin demo_db

# cdc check
cdc:
	@docker exec mongodb mongosh -u admin -p password --authenticationDatabase admin --eval "rs.status()" 



