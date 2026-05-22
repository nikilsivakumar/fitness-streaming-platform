"""
StreamBus — Local streaming simulation layer.

Mimics core broker concepts:
- Topics (named channels)
- Partition keys (ensures ordering per key, mirrors Kafka/Kinesis behavior)
- Consumer groups (multiple consumers can read independently)
- Persistent queue (records survive between producer/consumer calls)

Why this approach:
    Kafka and Kinesis are infrastructure concerns. The data engineering
    logic — schema validation, partitioning, enrichment, aggregation —
    is identical regardless of the broker. This layer lets us develop
    and test all pipeline logic locally without broker dependencies,
    then swap in Kinesis for the AWS version with zero logic changes.
"""

import queue
import threading
import json
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional


class StreamBus:
    """
    Thread-safe in-process message broker.
    
    Concepts mirrored from Kafka/Kinesis:
    - topic     → Kinesis stream name / Kafka topic
    - partition → Kinesis shard / Kafka partition  
    - offset    → position in stream (for replay)
    - consumer  → Lambda function / Kafka consumer group
    """

    def __init__(self, num_partitions: int = 3):
        self.num_partitions = num_partitions
        # topic → partition_id → list of records
        self._partitions: Dict[str, Dict[int, List]] = defaultdict(
            lambda: defaultdict(list)
        )
        # consumer_group → topic → partition → offset
        self._offsets: Dict[str, Dict[str, Dict[int, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        self._lock = threading.Lock()
        self.total_produced = 0
        self.total_consumed = 0

    def _get_partition(self, partition_key: str) -> int:
        """
        Deterministic partition assignment from partition key.
        
        Why this matters:
            In Kafka/Kinesis, the same partition key always routes
            to the same shard. This guarantees ordering — all events
            for user U1001 arrive in sequence. Critical for:
            - Computing rolling averages correctly
            - Detecting consecutive bad readings
            - Session reconstruction
        """
        return hash(partition_key) % self.num_partitions

    def produce(self, topic: str, record: dict, partition_key: str) -> dict:
        """
        Write a record to a topic partition.
        Mirrors: Kinesis.put_record() / KafkaProducer.send()
        """
        partition_id = self._get_partition(partition_key)
        
        envelope = {
            "data": record,
            "partition_key": partition_key,
            "partition_id": partition_id,
            "offset": None,          # assigned below
            "timestamp": datetime.utcnow().isoformat(),
            "topic": topic
        }

        with self._lock:
            partition = self._partitions[topic][partition_id]
            envelope["offset"] = len(partition)
            partition.append(envelope)
            self.total_produced += 1

        return envelope

    def consume(
        self,
        topic: str,
        consumer_group: str = "default",
        max_records: int = 100
    ) -> List[dict]:
        """
        Read unprocessed records from all partitions.
        Mirrors: Kinesis.get_records() / KafkaConsumer.poll()
        
        Consumer group concept:
            Each consumer group tracks its own offset independently.
            Two different consumers (e.g. Bronze writer + metrics counter)
            can both read the same stream without interfering.
            This mirrors Kinesis enhanced fan-out / Kafka consumer groups.
        """
        records = []

        with self._lock:
            for partition_id in range(self.num_partitions):
                partition = self._partitions[topic][partition_id]
                offset = self._offsets[consumer_group][topic][partition_id]
                
                batch = partition[offset: offset + max_records]
                records.extend(batch)
                
                # Advance offset — these records won't be re-delivered
                self._offsets[consumer_group][topic][partition_id] += len(batch)
                self.total_consumed += len(batch)

        return records

    def get_stats(self, topic: str) -> dict:
        """Broker health check — mirrors Kinesis DescribeStream."""
        with self._lock:
            total_records = sum(
                len(self._partitions[topic][p])
                for p in range(self.num_partitions)
            )
        return {
            "topic": topic,
            "partitions": self.num_partitions,
            "total_records": total_records,
            "total_produced": self.total_produced,
            "total_consumed": self.total_consumed
        }

    def reset_offsets(self, consumer_group: str, topic: str):
        """
        Reset consumer to beginning of stream.
        Mirrors Kinesis TRIM_HORIZON / Kafka auto.offset.reset=earliest.
        Used for reprocessing — one of the key benefits of immutable Bronze layer.
        """
        with self._lock:
            for partition_id in range(self.num_partitions):
                self._offsets[consumer_group][topic][partition_id] = 0


# Global broker instance shared across producer and consumer
# In AWS version this is replaced by the Kinesis stream name string
bus = StreamBus(num_partitions=3)
TOPIC = "fitness-events"