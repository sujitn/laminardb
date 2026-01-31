#!/bin/bash
# Create Kafka topics for the ShopStream demo.
# Run after docker-compose up:
#   bash examples/shopstream/scripts/setup-kafka.sh

set -e

KAFKA_BROKER="${KAFKA_BROKERS:-localhost:9092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"

echo "Creating ShopStream Kafka topics on $KAFKA_BROKER..."

# Input topics
for topic in clickstream orders inventory_updates; do
    echo "  Creating topic: $topic"
    docker exec shopstream-kafka \
        kafka-topics.sh --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --if-not-exists
done

# Output topics
for topic in session-analytics sales-kpis fraud-alerts inventory-alerts \
             trending-products user-segments revenue-by-category; do
    echo "  Creating topic: $topic"
    docker exec shopstream-kafka \
        kafka-topics.sh --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --if-not-exists
done

echo "Done. Topics:"
docker exec shopstream-kafka \
    kafka-topics.sh --list --bootstrap-server "$KAFKA_BROKER"
