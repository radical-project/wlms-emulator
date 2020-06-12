import radical.utils as ru
from ..entities.task import Task
from ..entities.core import Core
from yaml import load
import pika
import json
import os


class Executor(object):

    def __init__(self, cfg_path):

        self._uid = ru.generate_id('executor', mode=ru.ID_UNIQUE)
        self._logger = ru.Logger('radical.executor.%s' % self._uid)

        self._schedule = None
        self._profile = dict()
        self._engine_uid = None

        with open(cfg_path, 'r') as stream:
            cfg = load(stream)
            self._parse_cfg(cfg)

        self._setup_msg_sys()

    def _parse_cfg(self, cfg):

        self._host = cfg['rmq']['host']
        self._port = cfg['rmq']['port']
        self._id = cfg['rmq']['id']
        self._password = cfg['rmq']['password']
        self._exchange = cfg['rmq']['executor']['exchange']
        self._wlms_exchange = cfg['rmq']['wlms']['exchange']
        self._queue_schedule = cfg['rmq']['executor']['queues']['schedule']
        self._queue_cfg = cfg['rmq']['executor']['queues']['config']
        self._profile_loc = cfg['rmq']['executor']['profile_loc']
        self._logger.info('Configuration parsed')
        self._res_dyn = cfg['criteria']['res_dynamism']
        self._res_het = cfg['criteria']['res_het']
        self._wl_het = cfg['criteria']['wl_het']

    def _setup_msg_sys(self):

        credentials = pika.PlainCredentials(self._id, self._password)
        
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))
        chan = conn.channel()

        chan.exchange_declare(exchange=self._exchange, exchange_type='direct')

        chan.queue_declare(queue=self._queue_schedule)
        chan.queue_bind(queue=self._queue_schedule,
                        exchange=self._exchange, routing_key='schedule')

        chan.queue_declare(queue=self._queue_cfg)
        chan.queue_bind(queue=self._queue_cfg,
                        exchange=self._exchange, routing_key='cfg')

        self._logger.info('Messaging system established')

    def run(self):

        conn = None

        try:
            credentials = pika.PlainCredentials(self._id, self._password)
        
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))

            chan = conn.channel()

            cfg_msg = None
            cfg = None

            if self._res_dyn and not self._res_het and not self._wl_het:
                cores_as_dict = dict()

            while True:

                method_frame, header_frame, cfg_msg = chan.basic_get(queue=self._queue_cfg, auto_ack=True)

                if cfg_msg:

                    tasks = list()
                    cfg = cfg_msg
                    cfg_as_dict = json.loads(cfg)
                    if 'engine_uid' in cfg_as_dict.keys():
                        self._engine_uid = cfg_as_dict['engine_uid']

                    self._logger.info('Engine uid received')

                method_frame, header_frame, schedule = chan.basic_get(queue=self._queue_schedule, auto_ack=True)

                if schedule:
                    tasks = list()
                    schedule_as_dict = json.loads(schedule)
                     
                    if not (self._res_dyn and not self._res_het and not self._wl_het):
                        cores_as_dict = dict()

                    for s in schedule_as_dict:
                        task = Task(no_uid=True)
                        task.from_dict(s['task'])

                        if s['core']['uid'] not in cores_as_dict.keys():
                            core = Core(no_uid=True)
                            core.from_dict(s['core'])
                            cores_as_dict[s['core']['uid']] = core
                        else:
                            core = cores_as_dict[s['core']['uid']]
                            if self._res_dyn and not self._res_het and not self._wl_het:
                                core_temp = Core(no_uid=True)
                                core_temp.from_dict(s['core'])
                                core._perf = core_temp._perf

                        core.execute(task)
                        tasks.append(task)
                        
                    cores_as_list = [core.to_dict() for core in cores_as_dict.values()]
                    chan.basic_publish( exchange=self._wlms_exchange,
                                        routing_key='exec',
                                        body=json.dumps(cores_as_list)
                                    )

                    self._logger.info('Schedule executed')

                if schedule and cfg:

                    self._record_profile(
                        tasks=tasks, engine_uid=self._engine_uid)
                    self._logger.info('Execution profile recorded')

        except KeyboardInterrupt:

            if conn:
                # delete the queue generated to avoid possible conflict in the RMQ server 
                chan.queue_delete(queue = 'schedule')
                conn.close()
            self._write_profile()

            self._logger.info('Closing %s' % self._uid)

        except Exception as ex:

            if conn:
                # delete the queue generated to avoid possible conflict in the RMQ server 
                chan.queue_delete(queue = 'schedule')
                conn.close()

            self._logger.exception('Executor failed with %s' % ex)

    def _record_profile(self, tasks, engine_uid):

        if engine_uid not in self._profile.keys():
            self._profile[engine_uid] = list()

        for task in tasks:
            prof = {
                'task': task.uid,
                'ops': task.ops,
                'core': task.exec_core,
                'start_time': task.start_time,
                'end_time': task.end_time,
                'exec_time': task.end_time - task.start_time
            }
            self._profile[engine_uid].append(prof)

    def _write_profile(self):

        base = os.path.dirname(self._profile_loc)
        fname, ext = os.path.basename(self._profile_loc).split('.')
        op_name = base + '/' + fname + '.%s.' % self._uid + ext

        ru.write_json(data=self._profile, filename=op_name)
        self._logger.info(
            'Profiles from executor %s written to %s' % (self._uid, op_name))
