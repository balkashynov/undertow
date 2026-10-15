"""Instrumented real Arroyo worker. Native lifecycle logging is unchanged."""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from arroyo.backends.kafka.configuration import build_kafka_consumer_configuration
from arroyo.backends.kafka.consumer import KafkaConsumer
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing.processor import StreamProcessor
from arroyo.processing.strategies import CommitOffsets, RunTask
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.types import Topic


parser = argparse.ArgumentParser()
parser.add_argument("--broker", required=True)
parser.add_argument("--group", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--name", required=True)
parser.add_argument("--arm", required=True)
parser.add_argument("--delay", type=float, default=0)
parser.add_argument("--crash", action="store_true")
parser.add_argument("--auto", action="store_true")
args = parser.parse_args()


class JSONLog(logging.Handler):
    def emit(self, record):
        obj = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "consumer": args.name,
            "group_id": args.group,
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage()
            .replace(str(Path(__file__).resolve().parents[1]), "<checkout>")
            .replace(str(Path.home()), "<home>"),
        }
        if record.exc_info:
            obj["traceback"] = (
                logging.Formatter()
                .formatException(record.exc_info)
                .replace(str(Path(__file__).resolve().parents[1]), "<checkout>")
                .replace(str(Path.home()), "<home>")
            )
        if hasattr(record, "undertow"):
            obj["undertow"] = record.undertow
        if hasattr(record, "message_id"):
            obj["message_id"] = record.message_id
        if hasattr(record, "elapsed_ms"):
            obj["elapsed_ms"] = record.elapsed_ms
        print(json.dumps(obj), flush=True)


logging.getLogger().handlers = [JSONLog()]
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger("undertow.lab")


def extension(kind, partitions, **kwargs):
    return {
        "undertow": {
            "kind": kind,
            "partitions": [
                {"topic": p.topic.name, "partition": p.index, "offset": offset}
                for p, offset in partitions.items()
            ],
        },
        **kwargs,
    }


class ObservedConsumer(KafkaConsumer):
    def commit_offsets(self):
        result = super().commit_offsets()
        if result:
            logger.info(
                "Manual commit returned nonempty successful broker offsets",
                extra=extension("commit_confirmed", result),
            )
        return result


class DrainWrapper(ProcessingStrategy):
    def __init__(self, inner):
        self.inner = inner

    def submit(self, message):
        self.inner.submit(message)

    def poll(self):
        self.inner.poll()

    def close(self):
        self.inner.close()

    def terminate(self):
        self.inner.terminate()

    def join(self, timeout=None):
        if args.delay and Path(args.arm).exists():
            start = time.monotonic()
            time.sleep(args.delay)
            logger.info(
                "Controlled strategy join delay finished",
                extra={"elapsed_ms": (time.monotonic() - start) * 1000},
            )
        self.inner.join(timeout)


class Factory(ProcessingStrategyFactory):
    def create_with_partitions(self, commit, partitions):
        def process(message):
            value = json.loads(message.payload.value)
            if args.crash and Path(args.arm).exists():
                raise RuntimeError(
                    "Deliberate lab processing failure; synthetic workload only"
                )
            logger.info(
                "Processed synthetic message %s",
                value["id"],
                extra=extension(
                    "processed", message.committable, message_id=value["id"]
                ),
            )
            return message.payload

        return DrainWrapper(RunTask(process, CommitOffsets(commit)))


config = build_kafka_consumer_configuration(
    default_config={},
    group_id=args.group,
    bootstrap_servers=[args.broker],
    auto_offset_reset="earliest",
    enable_auto_commit=args.auto,
    override_params={
        "group.protocol": "consumer",
        "max.poll.interval.ms": 30000,
        "auto.commit.interval.ms": 500,
        "log_level": 3,
    },
)
consumer = ObservedConsumer(config)
processor = StreamProcessor(
    consumer,
    Topic(args.topic),
    Factory(),
    commit_policy=ONCE_PER_SECOND,
    stuck_detector_timeout=2,
)
signal.signal(signal.SIGTERM, lambda *_: processor.signal_shutdown())
signal.signal(signal.SIGINT, lambda *_: processor.signal_shutdown())
processor.run()
