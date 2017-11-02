import datetime
import logging
import os
import time
from collections import defaultdict
from threading import Thread

from celery import Celery
from celery.app.control import Control
from celery.events import EventReceiver
from celery.events.snapshot import Polaroid
from celery.events.state import State

from broker import Redis
from metric import metric

debug = os.environ.get('DEBUG', False) in ['true', 'yes', '1', 'True', 'Yes', 'Y', 'y']
logging.basicConfig(level=logging.INFO if not debug else logging.DEBUG)

logger = logging.getLogger(__name__)

TASK_METRIC_NAME = 'celery_task'
QUEUE_METRIC_NAME = 'celery_queue'


def reavg(old, count, delta):
    return (old * count + delta) / (count + 1)


class CeleryConfig(object):
    BROKER_URL = os.environ.get('CELERY_BROKER_URL')
    CELERY_ENABLE_UTC = True


class CeleryConsumer(Thread):
    def __init__(self, state, celery):
        super(CeleryConsumer, self).__init__()
        self.celery = celery
        self.state = state

    def run(self):
        with self.celery.connection() as conn:
            self.receiver = EventReceiver(
                conn,
                handlers={'*': self.state.event},
                app=self.celery,
            )
            self.receiver.capture()


class CeleryRecorder(Polaroid):
    clear_after = True
    redis = Redis(CeleryConfig.BROKER_URL)

    def on_shutter(self, state):
        try:
            self.gather_data(state)
        except Exception as ex:
            logger.exception(str(ex))

    def gather_data(self, state):
        tasks = self.gather_tasks(state)
        if not tasks:
            logger.error('Palariod ran out of drafts. No data collected. Will die now.')
            os._exit(os.EX_TEMPFAIL)

        self.report_tasks(tasks)
        self.report_queues()

    def gather_tasks(self, state):
        counts = defaultdict(  # Worker
            lambda: defaultdict(  # Task name
                lambda: defaultdict(  # Task state
                    lambda: dict({
                        'count': 0,
                        'avg_wait': 0.,
                        'max_wait': 0.,
                        'avg_exec': 0.,
                        'max_exec': 0.,
                    })
                )
            )
        )

        tasks = 0

        # Gather stats
        for uuid, task in state.tasks_by_time():
            tasks += 1

            worker = task.worker.hostname if task.worker else ''
            if worker and '@' in worker:
                worker = worker.split('@', 1)[1]  # celery@<hostname> -> <hostname>

            obj = counts[worker][task.name][task.state]
            obj['count'] += 1

            if task.eta is None and task.started and task.received and task.succeeded:
                wait = task.started - task.received
                exc = task.succeeded - task.started
                obj['avg_wait'] = reavg(obj['avg_wait'], obj['count'] - 1, wait)
                if wait > obj['max_wait']:
                    obj['max_wait'] = wait

                obj['avg_exec'] = reavg(obj['avg_exec'], obj['count'] - 1, exc)
                if exc > obj['max_exec']:
                    obj['max_exec'] = exc

        logger.debug("Gathered %s celery tasks", tasks)

        return counts

    def report_tasks(self, counts):
        for worker, tasks in counts.items():
            for name, states in tasks.items():
                for state, counts in states.items():
                    values = dict(
                        count=counts['count'],
                        avg_exec_in_millis=counts['avg_exec'],
                        max_exec_in_millis=counts['max_exec'],
                        avg_wait_in_millis=counts['avg_wait'],
                        max_wait_in_millis=counts['max_wait'],
                    )
                    tags = dict(
                        name=name,
                        worker=worker,
                        state=state,
                    )
                    logger.debug(f'Reporting tags: {tags} with values: {values}')
                    metric(TASK_METRIC_NAME, values, tags)

    def report_queues(self):
        for name, count in self.redis.itercounts():
            logger.debug(f'Reporting queue {name} with count {count}')
            metric(QUEUE_METRIC_NAME, {'count': count}, {'queue': name})


class CeleryQueue(object):
    def __init__(self):
        self.celery = Celery()
        self.celery.config_from_object(CeleryConfig)
        self.control = Control(self.celery)
        self.enable_event = datetime.datetime(2010, 1, 1)

        self.state = State()
        self.last_query = datetime.datetime.utcnow()

        self.consumer = CeleryConsumer(self.state, self.celery)

    def run(self):
        self.consumer.start()

        logger.info("Celery monitor started")

        freq = float(os.environ.get('FREQUENCY', 10))
        with CeleryRecorder(self.state, freq=freq):
            while True:
                time.sleep(.1)

                # Periodically re-enable celery control events
                if (datetime.datetime.utcnow() - self.enable_event).total_seconds() > 600:
                    self.control.enable_events()
                    self.enable_event = datetime.datetime.utcnow()


if __name__ == '__main__':
    try:
        CeleryQueue().run()
    except KeyboardInterrupt:
        os._exit(os.EX_USAGE)
