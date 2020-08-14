import radical.utils as ru
from ..entities.task import Task
from ..entities.core import Core
from yaml import load
import pika
import json
import os
from algorithms.spatial_binding_algos import largest_to_first_available


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
        self._early_binding = cfg['wlms']['early_binding']
        self._core_dist_type = cfg['criteria']['core_dist']
        self._core_dist_mean = cfg['criteria']['core_mean']
        self._core_dist_temp_var = cfg['criteria']['core_temp_var']
        if 'l2fare' in cfg['criteria']:
            self._l2fare = cfg['criteria']['l2fare']
        else:
            self._l2fare = False
        if 'res_dynamism' in cfg['criteria']: 
            self._res_dyn = cfg['criteria']['res_dynamism']
        else:
            self._res_dyn = False

    def _setup_msg_sys(self):

        credentials = pika.PlainCredentials(self._id, self._password)
        
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))
        chan = conn.channel()

        chan.exchange_declare(exchange=self._exchange, exchange_type='direct')

        chan.queue_declare(queue=self._queue_schedule)
        chan.queue_bind(queue=self._queue_schedule,
                        exchange=self._exchange, routing_key='schedule_as')

        chan.queue_declare(queue=self._queue_cfg)
        chan.queue_bind(queue=self._queue_cfg,
                        exchange=self._exchange, routing_key='cfg_as')

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

            if self._res_dyn:
                cores_as_dict = dict()

            while True:

                method_frame, header_frame, cfg_msg = chan.basic_get(queue=self._queue_cfg) #,
                                                                     # no_ack=True)
                if cfg_msg:

                    tasks = list()
                    cfg = cfg_msg
                    cfg_as_dict = json.loads(cfg)
                    if 'engine_uid' in cfg_as_dict.keys():
                        self._engine_uid = cfg_as_dict['engine_uid']

                    self._logger.info('Engine uid received')

                method_frame, header_frame, schedule = chan.basic_get(queue=self._queue_schedule) # ,
                                                                      #no_ack=True)

                if schedule:
                    tasks = list()
                    schedule_as_dict = json.loads(schedule)
                     
                    if not self._res_dyn:
                        cores_as_dict = dict()

                    if self._l2fare and not self._early_binding:
                        if schedule_as_dict[0]['core']['uid'] in cores_as_dict.keys():
                            schedule_as_dict = largest_to_first_available(schedule_as_dict, cores_as_dict)

                    for s in schedule_as_dict:
                        task = Task(no_uid=True)
                        task.from_dict(s['task'])

                        #print "--------------------------------------------------------"
                        if s['core']['uid'] not in cores_as_dict.keys():
                            #print "-------------------1st gen------------------------------"
                            #print "task ops ", s['task']['uid']
                            core = Core(no_uid=True)
                            #print "s[core] ", s['core']
                            core.from_dict(s['core'])
                            cores_as_dict[s['core']['uid']] = core
                            #print "core perf ", core._perf
                        else:
                            #print "-------------------2nd gen------------------------------"
                            #print "task uid: ", s['task']['uid']
                            core = cores_as_dict[s['core']['uid']]
                            if self._res_dyn and not self._l2fare:
                                if not self._early_binding:
                                    #print(s['core'])
                                    #print(core._task_history)
                                    core_temp = Core(no_uid=True)
                                    core_temp.from_dict(s['core'])
                                    #print(core._perf)
                                    #print(core_temp._perf)
                                    core._perf = core_temp._perf
                                else:
                                    core.refresh_perf(self._core_dist_type, self._core_dist_mean, self._core_dist_temp_var)
                        #print "CORE UID: ", s['core']['uid']
                        #print "CORE'S TASK HISTORY", core._task_history
                        #print "CORE PERF:", core._perf
                        #print "TASK UID:", task.uid
                        #print "TASK LENGTH:", task.ops

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
                chan.queue_delete(queue = 'schedule_as')
                conn.close()
            self._write_profile()

            self._logger.info('Closing %s' % self._uid)

        except Exception as ex:

            if conn:
                # delete the queue generated to avoid possible conflict in the RMQ server 
                chan.queue_delete(queue = 'schedule_as')
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
