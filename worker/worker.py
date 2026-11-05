
import json
import os
import time

import httpx
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "storage")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "storagepass")
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://nginx:80")
API_KEY = os.getenv("API_KEY", "midterm-secret")
QUEUE_NAME = "storage_maintenance"


def process_job(job):
    job_type = job.get("type")
    job_id = job.get("job_id", "unknown")

    endpoints = {
        "repair": "/cluster/repair",
        "rebalance": "/cluster/rebalance",
        "garbage_collection": "/cluster/cleanup",
    }

    if job_type not in endpoints:
        raise ValueError(f"Unsupported job type: {job_type}")

    print(f"[WORKER] Starting job {job_id}: {job_type}", flush=True)

    with httpx.Client(timeout=300) as client:
        response = client.post(
            f"{GATEWAY_URL}{endpoints[job_type]}",
            headers={"X-API-Key": API_KEY},
        )
        response.raise_for_status()
        result = response.json()

    print(f"[WORKER] {job_type} result: {result}", flush=True)

    if (
        result.get("failed_repairs", 0)
        or result.get("failed_operations", 0)
        or result.get("pending_chunks", 0)
    ):
        raise RuntimeError(f"Maintenance job incomplete: {result}")

    print(f"[WORKER] Completed job {job_id}", flush=True)


def main():
    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD
    )

    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    while True:
        try:
            print("[WORKER] Connecting to RabbitMQ...", flush=True)

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True
            )

            channel.basic_qos(prefetch_count=1)

            print(
                f"[WORKER] Waiting for jobs on {QUEUE_NAME}",
                flush=True
            )

            def callback(ch, method, properties, body):
                try:
                    job = json.loads(body.decode("utf-8"))
                    process_job(job)

                    ch.basic_ack(
                        delivery_tag=method.delivery_tag
                    )

                except Exception as exc:
                    print(f"[WORKER] Job failed: {exc}", flush=True)

                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False
                    )

            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False,
            )

            channel.start_consuming()

        except (pika.exceptions.AMQPError, OSError) as exc:
            print(f"[WORKER] Connection error: {exc}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
