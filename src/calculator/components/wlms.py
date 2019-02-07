import radical.utils as ru
from .selectors.core_selector import Core_Selector
from .selectors.task_selector import Task_Selector
from .binders.spatial_binder import Spatial_Binder
from .binders.temporal_binder import Temporal_Binder
from ..api.resource import Resource
from ..api.workload import Workload
from yaml import load
import pika
import json
from ..exceptions import CalcError


class WLMS(object):

    def __init__(self,
                 cfg_path):

        self._uid = ru.generate_id('wlms', mode=ru.ID_UNIQUE)
        self._logger = ru.Logger('radical.wlms.%s' % self._uid)
        self._ts_criteria = None
        self._rs_criteria = None
        self._sb_criteria = None
        self._tb_criteria = None
        self._host = None
        self._port = None

        self._workload = None
        self._resource = None
        self._early_binding = None

        with open(cfg_path, 'r') as stream:
            cfg = load(stream)
            self._parse_cfg(cfg)

        self._setup_msg_sys()

    def _parse_cfg(self, cfg):

        self._host = cfg['rmq']['host']
        self._port = cfg['rmq']['port']
        self._exchange = cfg['rmq']['wlms']['exchange']
        self._tgt_exchange = cfg['rmq']['executor']['exchange']
        self._wl_queue = cfg['rmq']['wlms']['queues']['workload']
        self._res_queue = cfg['rmq']['wlms']['queues']['resource']
        self._exec_queue = cfg['rmq']['wlms']['queues']['executor']
        self._early_binding = cfg['wlms'].get('early_binding', False)

        self._set_criteria(cfg['criteria'])
        self._logger.info('Configuration parsed')

    def _set_criteria(self, cfg):

        valid_ts_criteria = ['all']
        valid_rs_criteria = ['all']
        valid_sb_criteria = ['rr', 'l2f', 's2f', 'random']
        valid_tb_criteria = ['ff', 'sf', 'random']

        task_selection_criteria = cfg['task_selector']
        resource_selection_criteria = cfg['resource_selector']
        spatial_binding_criteria = cfg['spatial_binder']
        tempora_binding_criteria = cfg['temporal_binder']

        if task_selection_criteria not in valid_ts_criteria or \
                resource_selection_criteria not in valid_rs_criteria or \
                spatial_binding_criteria not in valid_sb_criteria or \
                tempora_binding_criteria not in valid_tb_criteria:
            raise CalcError('%s, %s, %s, or %s is invalid' % (task_selection_criteria,
                                                            resource_selection_criteria,
                                                            spatial_binding_criteria,
                                                            tempora_binding_criteria))

        self._ts_criteria = task_selection_criteria
        self._rs_criteria = resource_selection_criteria
        self._sb_criteria = spatial_binding_criteria
        self._tb_criteria = tempora_binding_criteria

        self._logger.info('Criteria assigned')

    def _setup_msg_sys(self):

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port))
        chan = conn.channel()

        chan.exchange_declare(exchange=self._exchange, exchange_type='direct')

        chan.queue_declare(queue=self._wl_queue)
        chan.queue_declare(queue=self._res_queue)
        chan.queue_declare(queue=self._exec_queue)

        chan.queue_bind(queue=self._wl_queue,
                        exchange=self._exchange, routing_key='wl')
        chan.queue_bind(queue=self._res_queue,
                        exchange=self._exchange, routing_key='res')
        chan.queue_bind(queue=self._exec_queue,
                        exchange=self._exchange, routing_key='exec')

        conn.close()

        self._logger.info('Messaging system established')

    def run(self):

        conn = None

        try:

            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host, port=self._port))
            chan = conn.channel()

            def create_schedule(s_mapping, t_mapping):

                schedule = list()
                for t1 in t_mapping:
                    for item in s_mapping:
                        if t1.uid == item['task'].uid:
                            schedule.append(item)

                return schedule

            resource_visited = list()

            while True:

                method_frame, header_frame, wl = chan.basic_get(queue=self._wl_queue,
                                                                no_ack=True)
                if wl and not self._workload:
                    wl_as_dict = json.loads(wl)
                    submit_time = wl_as_dict.pop('submit_time')

                    self._workload = Workload(no_uid=True)
                    self._workload.from_dict(wl_as_dict)
                    self._logger.info('Workload %s received' %
                                      self._workload.uid)

                method_frame, header_frame, res = chan.basic_get(queue=self._res_queue,
                                                                 no_ack=True)
                if res:
                    self._resource = Resource(no_uid=True)
                    self._resource.from_dict(json.loads(res))
                    self._logger.info('Resource %s received' %
                                      self._resource.uid)

                if self._workload and self._resource:

                    if not self._early_binding or (self._resource.uid not in resource_visited):
                        self._resource.create_core_list()
                        self._logger.info("Resource core list created")
                        if self._resource.uid not in resource_visited:
                            resource_visited.append(self._resource.uid)

                    sel = Core_Selector()
                    sel.criteria = self._rs_criteria
                    selected_cores = sel.select(collection=self._resource.core_list,
                                                count=self._resource.num_cores)

                    if not selected_cores:
                        self._logger.info(
                            'No cores selected in resource selection phase')
                        continue

                    self._logger.info('Resource selection complete')

                    sel = Task_Selector()
                    sel.criteria = self._ts_criteria
                    selected_tasks = sel.select(collection=self._workload.task_list,
                                                count=self._workload.num_tasks)

                    if not selected_tasks:
                        self._logger.info(
                            'No tasks selected in task selection phase')
                        continue

                    self._logger.info('Task selection complete')

                    sb = Spatial_Binder()
                    sb.criteria = self._sb_criteria
                    s_mapping = sb.bind(workload=selected_tasks,
                                       resource=selected_cores)

                    if not s_mapping:
                        self._logger.info(
                            'No spatial mapping created by current binding cfg')
                        continue

                    self._logger.info('Spatial Binding complete')

                    tb = Temporal_Binder()
                    tb.criteria = self._tb_criteria
                    t_mapping = tb.bind(workload=selected_tasks)

                    if not t_mapping:
                        self._logger.info(
                            'No temporal mapping created by current binding cfg')
                        continue

                    self._logger.info('Temporal Binding complete')

                    schedule = create_schedule(s_mapping, t_mapping)

                    schedule_as_dict = list()                    
                    for s in schedule:
                        task_as_dict = s['task'].to_dict()
                        core_as_dict = s['core'].to_dict()
                        tmp = {'task': task_as_dict, 'core': core_as_dict}
                        schedule_as_dict.append(tmp)

                    chan.basic_publish(exchange=self._tgt_exchange,
                                       routing_key='schedule',
                                       body=json.dumps(schedule_as_dict))

                    self._logger.info('Schedule published to executor')

                    self._workload = None
                    cores = None

                    while not cores:
                        method_frame, header_frame, cores = chan.basic_get(queue=self._exec_queue,
                                                                        no_ack=True)

                    self._logger.info('Received updates cores')
                    cores_as_dict = json.loads(cores)
                    for core in cores_as_dict:
                        for c in selected_cores:
                            if core['uid'] == c.uid:
                                c.util = core['util']
                                c.task_history = core['task_history']

                    self._logger.info('Cores updated')

        except KeyboardInterrupt:

            if conn:
                conn.close()

            self._logger.info('Closing %s' % self._uid)

        except Exception as ex:

            if conn:
                conn.close()

            self._logger.exception('WLMS failed with %s' % ex)
