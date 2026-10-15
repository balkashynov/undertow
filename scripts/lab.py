"""Real Kafka integration lab. Only creates uniquely named local test resources."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from undertow.core import analyze, ingest

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"


def command(args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()


def wait_for(fn, description, timeout=50):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fn():
            return
        time.sleep(0.2)
    raise RuntimeError("Timed out: " + description)


def records(paths):
    result = []
    for path in paths:
        for line in path.read_text().splitlines() if path.exists() else []:
            try:
                result.append(json.loads(line))
            except ValueError:
                pass
    return result


def scenario(broker, kind, out):
    out.mkdir(parents=True, exist_ok=True)
    suffix = uuid.uuid4().hex[:8]
    topic = "undertow-" + suffix
    group = "lab-" + kind
    # Broker is unique per invocation; topic/group captures remain isolated.
    admin = AdminClient({"bootstrap.servers": broker})
    for future in admin.create_topics(
        [NewTopic(topic, num_partitions=6, replication_factor=1)]
    ).values():
        future.result(15)
    producer = Producer({"bootstrap.servers": broker})
    arm = out / "delay.arm"
    arm.unlink(missing_ok=True)
    workers = []
    files = []
    paths = []
    acks = []

    def worker(name, delay=0, crash=False):
        path = out / (name + ".jsonl")
        fh = path.open("w")
        files.append(fh)
        paths.append(path)
        args = [
            sys.executable,
            str(ROOT / "scripts/lab_worker.py"),
            "--broker",
            broker,
            "--topic",
            topic,
            "--group",
            group,
            "--name",
            name,
            "--arm",
            str(arm),
            "--delay",
            str(delay),
        ]
        if kind == "auto":
            args += ["--auto"]
        if crash:
            args += ["--crash"]
        p = subprocess.Popen(args, stdout=fh, stderr=subprocess.PIPE, text=True)
        workers.append(p)
        return p

    def send(first, last):
        errors = []

        def callback(err, msg):
            if err:
                errors.append(str(err))
            else:
                acks.append({"partition": msg.partition(), "offset": msg.offset()})

        for n in range(first, last):
            producer.produce(
                topic,
                value=json.dumps({"id": n}).encode(),
                partition=n % 6,
                on_delivery=callback,
            )
        if producer.flush(15) or errors:
            raise RuntimeError("Producer delivery failed: " + str(errors))

    def seen():
        return {r["message_id"] for r in records(paths) if "message_id" in r}

    try:
        worker("consumer-a", 3 if kind == "slow-drain" else 0)
        worker("consumer-b", 3 if kind == "slow-drain" else 0)
        send(0, 48)
        wait_for(lambda: len(seen()) == 48, "first 48 messages processed")
        arm.touch()
        joining = worker("consumer-c", crash=kind == "crash")
        wait_for(
            lambda: any(
                r.get("consumer") == "consumer-c"
                and r.get("message", "").startswith("New partitions assigned:")
                and "Partition(" in r["message"]
                for r in records(paths)
            ),
            "joining consumer assignment",
        )
        send(48, 96)
        if kind == "crash":
            wait_for(lambda: joining.poll() is not None, "intentional crash")
            if joining.returncode == 0:
                raise RuntimeError("Crash negative control unexpectedly succeeded")
            arm.unlink(missing_ok=True)
            worker("consumer-c-restarted")
        wait_for(lambda: len(seen()) == 96, "all 96 message IDs observed", 70)
        # Independent read of broker-committed next offsets, not a parser assertion.
        probe = Consumer(
            {
                "bootstrap.servers": broker,
                "group.id": group,
                "enable.auto.commit": False,
            }
        )
        observed = []

        def committed():
            nonlocal observed
            observed = probe.committed(
                [TopicPartition(topic, i) for i in range(6)], timeout=5
            )
            return all(p.offset == 16 for p in observed)

        wait_for(committed, "broker reports all six next offsets at 16", 20)
        probe.close()
        time.sleep(0.3)
    finally:
        arm.unlink(missing_ok=True)
        for p in workers:
            if p.poll() is None:
                p.terminate()
        for p in workers:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for i, p in enumerate(workers):
            err = p.stderr.read() if p.stderr else ""
            if err:
                (out / f"worker-{i}-stderr.log").write_text(
                    err.replace(str(ROOT), "<checkout>").replace(
                        str(Path.home()), "<home>"
                    )
                )
        for fh in files:
            fh.close()
    raw = records(paths)
    sources = [{"name": p.name, "content": p.read_text()} for p in paths]
    labels = {
        "baseline": "Healthy cooperative rebalance",
        "slow-drain": "The three-second strategy join",
        "crash": "A processing crash and replacement",
        "auto": "Auto-commit: the log is not the commit",
    }
    case = ingest(sources, labels[kind])
    case["provenance"] = {
        "type": "recorded-local-execution",
        "description": f"Real Apache Kafka 4.1.0 + Arroyo 2.46.0, local Docker broker and macOS workers. Synthetic 96-message workload. Scenario: {kind}.",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "scenario": kind,
        "image": IMAGE,
        "arroyo_version": "2.46.0",
    }
    counts = Counter(r["message_id"] for r in raw if "message_id" in r)
    joins = [
        f["end"] - f["start"]
        for f in case["analysis"]["findings"]
        if f["code"] == "strategy_drain"
    ]
    result = {
        "scenario": kind,
        "producer_acks": len(acks),
        "unique_processed_ids": len(counts),
        "duplicate_processing_observations": sum(n - 1 for n in counts.values()),
        "missing_ids": sorted(set(range(96)) - set(counts)),
        "broker_next_offsets": [
            {"partition": p.partition, "offset": p.offset} for p in observed
        ],
        "max_observed_join_ms": max(joins, default=None),
        "injected_delay_ms": 3000 if kind == "slow-drain" else 0,
        "independent_delay_measurements_ms": [
            r["elapsed_ms"] for r in raw if "elapsed_ms" in r
        ],
        "counts": case["analysis"]["counts"],
        "rejected_lines": len(case["diagnostics"]),
        "source_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths
        },
    }
    assert len(acks) == 96 and len(counts) == 96 and not result["missing_ids"]
    assert all(p.offset == 16 for p in observed)
    assert not case["diagnostics"]
    if kind == "slow-drain":
        assert (
            result["max_observed_join_ms"] >= 2900
            and result["independent_delay_measurements_ms"]
        )
    if kind == "crash":
        assert case["analysis"]["counts"].get("crash", 0) >= 1
    else:
        assert not case["analysis"]["counts"].get("crash", 0)
    if kind == "auto":
        assert case["analysis"]["counts"].get("commit_attempt", 0) > 0 and not case[
            "analysis"
        ]["counts"].get("commit_confirmed", 0)
    (out / "incident.json").write_text(json.dumps(case, indent=2))
    (out / "verification.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)
    return case, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/lab")
    parser.add_argument(
        "--scenarios",
        nargs="+",
        choices=["baseline", "slow-drain", "crash", "auto"],
        default=["baseline", "slow-drain", "crash", "auto"],
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    name = "undertow-lab-" + uuid.uuid4().hex[:10]
    broker = f"127.0.0.1:{port}"
    env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{broker}",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "1000",
        "KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS": "1000",
        "KAFKA_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS": "6000",
        "KAFKA_GROUP_CONSUMER_SESSION_TIMEOUT_MS": "6000",
        "KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS": "1000",
        "KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS": "1000",
        "KAFKA_HEAP_OPTS": "-Xmx512M -Xms256M",
    }
    run = [
        "docker",
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        "org.undertow.lab=true",
        "--publish",
        f"127.0.0.1:{port}:9092",
    ]
    for k, v in env.items():
        run += ["--env", f"{k}={v}"]
    run.append(IMAGE)
    print("Starting isolated Kafka: " + name, flush=True)
    container = command(run)
    try:

        def ready():
            try:
                return bool(
                    AdminClient({"bootstrap.servers": broker, "log_level": 0})
                    .list_topics(timeout=1)
                    .brokers
                )
            except Exception:
                return False

        wait_for(ready, "Kafka startup", 60)
        results = []
        for kind in args.scenarios:
            print("Running " + kind, flush=True)
            case, result = scenario(broker, kind, args.output / kind)
            results.append(result)
            (ROOT / "src/undertow/web/demos" / f"{kind}.json").write_text(
                json.dumps(case, separators=(",", ":"))
            )
        (args.output / "summary.json").write_text(
            json.dumps(
                {
                    "image": IMAGE,
                    "results": results,
                    "limits": [
                        "Single local broker; no production Sentry systems",
                        "Synthetic workload; no throughput or availability claim",
                        "Python runtime only; KIP-848 consumer protocol",
                        "No power loss or network-partition simulation",
                    ],
                },
                indent=2,
            )
        )
    finally:
        (args.output / "broker.log").write_text(command(["docker", "logs", name]))
        command(["docker", "rm", "--force", name])
        print("Removed isolated Kafka container " + name, flush=True)


if __name__ == "__main__":
    main()
