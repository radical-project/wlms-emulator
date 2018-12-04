import radical.utils as ru
from ..components.selector import Selector
from ..components.binder import Binder
from ..components.executor import Executor
from ..api.resource import Resource
from ..api.workload import Workload
from ..exceptions import *
from yaml import load
import pika
import json

class WLMS(object):

    def __init__(self,
                 cfg_path):

        self._uid = ru.generate_id('wlms')
        self._logger = ru.Logger('radical.wlms.%s' % self._uid)
        self._workload = None
        self._resource = None
        self._schedule = list()
        self._ts_criteria = None
        self._rs_criteria = None
        self._b_criteria = None
        self._host = None
        self._port = None

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
        self._cfg_queue = cfg['rmq']['wlms']['queues']['config']

        self._set_criteria(cfg['criteria'])
        self._logger.info('Configuration parsed')

    def _set_criteria(self, cfg):

        valid_ts_criteria = ['all']
        valid_rs_criteria = ['all']
        valid_b_criteria = ['rr', 'tte', 'util','random']

        task_selection_criteria = cfg['task_selector']
        resource_selection_criteria = cfg['resource_selector']
        binding_criteria = cfg['binder']

        if task_selection_criteria not in valid_ts_criteria or \
                resource_selection_criteria not in valid_rs_criteria or \
                binding_criteria not in valid_b_criteria:
            raise CalcError('%s, %s, or %s is invalid' % (task_selection_criteria,
                                                          resource_selection_criteria,
                                                          binding_criteria))

        self._ts_criteria = task_selection_criteria
        self._rs_criteria = resource_selection_criteria
        self._b_criteria = binding_criteria

        self._logger.info('Criteria assigned')

    def _setup_msg_sys(self):

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port))
        chan = conn.channel()

        chan.exchange_declare(exchange=self._exchange, exchange_type='direct')

        chan.queue_declare(queue=self._wl_queue)
        chan.queue_declare(queue=self._res_queue)
        chan.queue_declare(queue=self._cfg_queue)

        chan.queue_bind(queue=self._wl_queue, exchange=self._exchange, routing_key='wl')
        chan.queue_bind(queue=self._res_queue, exchange=self._exchange, routing_key='res')
        chan.queue_bind(queue=self._cfg_queue, exchange=self._exchange, routing_key='cfg')

        conn.close()

        self._logger.info('Messaging system established')

    def run(self):

        conn = None

        try:

            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host, port=self._port))
            chan = conn.channel()

            workload, resource = None, None

            while True:

                method_frame, header_frame, wl = chan.basic_get(queue=self._wl_queue,
                                                                no_ack=True)
                if wl and not workload:
                    workload = Workload(no_uid=True)
                    workload.from_dict(json.loads(wl))
                    self._logger.info('Workload %s received'%workload.uid)

                method_frame, header_frame, res = chan.basic_get(queue=self._res_queue,
                                                                 no_ack=True)
                if res and not resource:
                    resource = Resource(no_uid=True)
                    resource.from_dict(json.loads(res))
                    self._logger.info('Resource %s received'%resource.uid)

                if workload and resource:

                    sel = Selector()
                    sel.criteria = self._ts_criteria
                    selected_tasks = sel.select(collection=workload.task_list,
                                                      count=workload.num_tasks)
                    self._logger.info('Task selection complete')

                    sel = Selector()
                    sel.criteria = self._rs_criteria
                    selected_cores = sel.select(collection=resource.core_list,
                                                      count=resource.num_cores)
                    self._logger.info('Resource selection complete')

                    m = Binder()
                    m.criteria = self._b_criteria
                    schedule = m.bind(selected_tasks, selected_cores)
                    self._logger.info('Binding complete')

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

                    workload, resource = None, None

                method_frame, header_frame, cfg = chan.basic_get(queue=self._cfg_queue,
                                                                 no_ack=True)
                if cfg:
                    self._logger.info('Reassigning criteria')
                    self._set_criteria(cfg)



        except KeyboardInterrupt:

            if conn:
                conn.close()

            self._logger.info('Closing %s'%self._uid)


        except Exception as ex:

            if conn:
                conn.close()

            self._logger.exception('WLMS failed with %s'%ex)
