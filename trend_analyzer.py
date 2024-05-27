from typing import Dict
from queue import Queue

from threading import Thread

from redis import Redis

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from metrics.metrics import Metrics

from microservice.microservice import Microservice


class TrendAnalyzer(Microservice):
    '''
    Класс отвечающий за представление Trend Analyzer
    Его задача -- по запросу от Observer Manager'a собирать метрики из Redis и строить по ним тренд
    '''

    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter], redis: Redis):
        '''
        Инициализация класса:
        - `redis` - подключение к базе данных Redis
        Поля класса:
        - `self.redis` - подключение к базе данных Redis
        '''

        self.redis = redis

        return super().__init__(event_queue, writers)

    def handle_event(self, event: Event):
        '''
        Обработка ивентов
        '''
        target_function = None

        match event.type:
            case EventType.AnalyseTrend:
                target_function = self.handle_event_analyse_trend
            case _:
                pass

        if target_function is not None:
            Thread(target=target_function, args=(event.data,)).start()

    def handle_event_analyse_trend(self, _event_data):
        '''
        Анализируем тренд по метрикам из рэдиса
        '''
        metrics: list[Metrics] = self.get_metrics_from_redis()

        trend: TrendData = self.analyse_trend(metrics)

        # ура тренд построен!
        self.writers['om'].send_event(Event(EventType.TrendData, trend))

    def get_metrics_from_redis(self) -> list[Metrics]:
        '''
        Собираем сколько может метрик из Redis
        '''
        metrics_values: list[Metrics] = []

        metrics_index: int = 0
        while True:
            metric_json = self.redis.get(f'{metrics_index}')
            if metric_json is not None: metrics_values.append(MetricsFromStr(metric_json))
            else: break
            metrics_index += 1

        return metrics_values

    def analyse_trend(self, metrics: list[Metrics]) -> TrendData:
        '''
        Строим тренд
        '''
        metrics_count: int = len(metrics)

        print(f'analysing metrics: {metrics}')
        for metric in metrics:
            print(str(metric))
 
        cpu_load: float = 0.0
        ram_load: float = 0.0
        net_in_load: float = 0.0
        net_out_load: float = 0.0

        for metric in metrics:
            cpu_load += metric.cpu_load / metrics_count
            ram_load += metric.ram_load / metrics_count
            net_in_load += metric.net_in_load / metrics_count
            net_out_load += metric.net_out_load / metrics_count

        return TrendData(cpu_load, ram_load, net_in_load, net_out_load)

