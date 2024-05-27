from redis import Redis
from prometheus_api_client import PrometheusConnect

from trend_analyzer import TrendAnalyzer

from events.kafka_event import *
from events.event import *

import os

# put the values in a k8s manifest
kafka_bootstrap_server: str | None = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
redis_host: str | None = os.environ.get('REDIS_HOST')
redis_port: str | None = os.environ.get('REDIS_PORT')

if kafka_bootstrap_server is None:
    print(f'Environment variable KAFKA_BOOTSTRAP_SERVER is not set')
    exit(1)

if redis_host is None:
    print(f'Environment variable REDIS_HOST is not set')
    exit(1)

if redis_port is None:
    print(f'Environment variable REDIS_PORT is not set')
    exit(1)


event_queue = Queue()

# share the queue with a reader
reader = KafkaEventReader(
    KafkaConsumer(
        'ta',
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset='latest',
    ),
    event_queue
)

# writers to other microservices
writers = {
    'om': KafkaEventWriter(
        KafkaProducer(
            bootstrap_servers=kafka_bootstrap_server
        ),
        'om'
    ),
}

# init the microservice
trend_analyzer = TrendAnalyzer(
    event_queue, writers,
    Redis(
        host=redis_host,
        port=int(redis_port),
    ),
)

trend_analyzer.running_thread.join()
