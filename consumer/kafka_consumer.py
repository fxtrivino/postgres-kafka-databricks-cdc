
import json
import os
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException, TopicPartition
from consumer.batch_writer import write_batch
from consumer.batch_recovery import (
    recover_events,
    split_contiguous_events
)

import time
import os

FLUSH_INTERVAL_SECONDS = 10

TOPIC = "postgres.public.customers"
OUTPUT_DIR = "data/cdc_batches"
BATCH_SIZE = 100


def build_event(message):
    raw_key = message.key()
    raw_value = message.value()

    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "kafka_timestamp_ms": message.timestamp()[1],
        "key": (
            json.loads(raw_key.decode("utf-8"))
            if raw_key is not None else None
        ),
        "value": (
            json.loads(raw_value.decode("utf-8"))
            if raw_value is not None else None
        ),
        "is_tombstone": raw_value is None
    }


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "cdc-json-exporter-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False
    })

    batches = defaultdict(list)

    last_flush = time.monotonic()

    simulate_failure = (
        os.getenv("SIMULATE_FAILURE_AFTER_EXPORT", "false").lower()
        == "true"
    )

    def flush_partition(partition):
        events = batches[partition]

        if not events:
            return

        new_events, recovered_count = recover_events(
            events,
            OUTPUT_DIR
        )

        print(
            f"Recovered events={recovered_count}, "
            f"new events={len(new_events)}"
        )

        segments = split_contiguous_events(new_events)

        if not segments:
            print("All events already published")

        for segment in segments:
            path = write_batch(segment, OUTPUT_DIR)

            print(
                f"Published: {path}, "
                f"offsets={segment[0]['offset']}-"
                f"{segment[-1]['offset']}"
            )   

        if simulate_failure:
            print(
                "SIMULATED_FAILURE_AFTER_EXPORT",
                flush=True
            )
            os._exit(1)

        next_offset = events[-1]["offset"] + 1

        consumer.commit(
            offsets=[
                TopicPartition(
                    TOPIC,
                    partition,
                    next_offset
                )
            ],
            asynchronous=False
        )

        print(
            f"Committed partition={partition}, "
            f"next_offset={next_offset}"
        )

        batches[partition].clear()

    def on_assign(consumer, partitions):
        print(f"Partitions assigned: {partitions}")


    def on_revoke(consumer, partitions):
        print(f"Partitions revoked: {partitions}")

        for tp in partitions:
            partition = tp.partition

            if batches.get(partition):
                flush_partition(partition)

            batches.pop(partition, None)

    def on_lost(consumer, partitions):
        print(f"Partitions lost: {partitions}")

        for tp in partitions:
            partition = tp.partition

            pending = len(batches.get(partition, []))

            if pending:
                print(
                    f"Discarding {pending} uncommitted "
                    f"events from partition={partition}. "
                    "They must be replayed by the new owner."
                )

            batches.pop(partition, None)

    try:

        consumer.subscribe(
            [TOPIC],
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost
        )

        print(f"Consuming topic: {TOPIC}")

        while True:
            message = consumer.poll(1.0)

            if message is not None:
                if message.error():
                    raise KafkaException(message.error())

                event = build_event(message)
                partition = event["partition"]

                batches[partition].append(event)

                if len(batches[partition]) >= BATCH_SIZE:
                    flush_partition(partition)

            now = time.monotonic()

            if now - last_flush >= FLUSH_INTERVAL_SECONDS:
                for partition in list(batches):
                    flush_partition(partition)

                last_flush = now

    except KeyboardInterrupt:
        print("Stopping consumer...")

        for partition in list(batches):
            flush_partition(partition)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
