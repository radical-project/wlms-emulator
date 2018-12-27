import radical.utils as ru
from ..entities.task import Task
from ..entities.core import Core
from ..exceptions import *
from yaml import load
import pika
import json
import os


class Executor(object):

    def __init__(self, cfg_path):

        self._uid = ru.generate_id('executor', mode=ru.ID_UNIQUE)
        self._logger = ru.Logger('radical.executor.%s' % self._uid)

        self._schedule = None
        self._profile = None
        self._engine_uid = None

        with open(cfg_path, 'r') as stream:
            cfg = load(stream)
            self._parse_cfg(cfg)

        self._setup_msg_sys()

    def _parse_cfg(self, cfg):

        self._host = cfg['rmq']['host']
        self._port = cfg['rmq']['port']
        self._exchange = cfg['rmq']['executor']['exchange']
        self._queue_schedule = cfg['rmq']['executor']['queues']['schedule']
        self._queue_cfg = cfg['rmq']['executor']['queues']['config']
        self._profile_loc = cfg['rmq']['executor']['profile_loc']
        self._logger.info('Configuration parsed')

    def _setup_msg_sys(self):

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port))
        chan = conn.channel()

        chan.exchange_declare(exchange=self._exchange, exchange_type='direct')

        chan.queue_declare(queue=self._queue_schedule)
        chan.queue_bind(queue=self._queue_schedule, exchange=self._exchange, routing_key='schedule')

        chan.queue_declare(queue=self._queue_cfg)
        chan.queue_bind(queue=self._queue_cfg, exchange=self._exchange, routing_key='cfg')

        self._logger.info('Messaging system established')


    def run(self):

        conn = None

        try:

            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host, port=self._port))
            chan = conn.channel()

            new_cfg = None
            new_schedule = None

            while True:

                method_frame, header_frame, cfg = chan.basic_get(queue=self._queue_cfg,
                                                                no_ack=True)
                if cfg:

                    tasks = list()
                    cfg_as_dict = json.loads(cfg)
                    if 'engine_uid' in cfg_as_dict.keys():
                        self._engine_uid = cfg_as_dict['engine_uid']

                    self._logger.info('Engine uid received')


                method_frame, header_frame, schedule = chan.basic_get(queue=self._queue_schedule,
                                                                no_ack=True)
                if schedule:

                    tasks = list()
                    schedule_as_dict = json.loads(schedule)

                    for s in schedule_as_dict:
                        task = Task(no_uid=True)
                        task.from_dict(s['task'])
                        core = Core(no_uid=True)
                        core.from_dict(s['core'])
                        core.execute(task)
                        tasks.append(task)

                    self._logger.info('Schedule executed')

                if schedule and cfg:

                    self._record_profile(tasks=tasks, engine_uid=self._engine_uid)
                    self._logger.info('Execution profile recorded')


        except KeyboardInterrupt:

            if conn:
                conn.close()
            self._write_profile()

            self._logger.info('Closing %s'%self._uid)


        except Exception as ex:

            if conn:
                conn.close()

            self._logger.exception('Executor failed with %s'%ex)


    def _record_profile(self, tasks, engine_uid):

        if engine_uid not in self._profile.keys():
            self._profile[engine_uid] = list()

        for task in tasks:
            prof = {
                        'task': task.uid,
                        'core': task.exec_core,
                        'start_time': task.start_time,
                        'end_time': task.end_time,
                        'exec_time': task.end_time - task.start_time
                    }
            self._profile[engine_uid].append(prof)

    def _write_profile(self):

        base = os.path.dirname(self._profile_loc)
        fname, ext = os.path.basename(self._profile_loc).split('.')
        op_name = base + '/' + fname + '.%s.'%self._uid + ext

        ru.write_json(data=self._profile, filename=op_name)
        self._logger.info('Profiles from executor %s written to %s'%(self._uid, op_name))