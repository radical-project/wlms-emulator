import radical.utils as ru
from ..components.selector import Selector
from ..components.binder import Binder
from ..components.executor import Executor
from ..exceptions import *
from yaml import load
import pika


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
        self._set_criteria(cfg['criteria'])
        self._logger.info('Configuration parsed')

    def _set_criteria(self, cfg):

        valid_ts_criteria = ['all']
        valid_rs_criteria = ['all']
        valid_b_criteria = ['rr', 'tte', 'util']

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

        chan.exchange_declare(exchange='wlms', exchange_type='direct')

        chan.queue_declare(queue='wl', exclusive=True)
        chan.queue_declare(queue='res', exclusive=True)
        chan.queue_declare(queue='cfg', exclusive=True)

        chan.queue_bind(queue='wl', exchange='wlms', routing_key='wl')
        chan.queue_bind(queue='res', exchange='wlms', routing_key='res')
        chan.queue_bind(queue='cfg', exchange='wlms', routing_key='cfg')

        self._logger.info('Messaging system established')

    def run(self):

        try:

            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host, port=self._port))
            chan = conn.channel()

            workload, resource = None, None

            while True:

                method_frame, header_frame, wl = chan.basic_get(queue='wl',
                                                                no_ack=True)
                if wl and not workload:
                    workload = wl.from_dict()
                    self._logger.info('Workload %s received'%workload.uid)

                method_frame, header_frame, res = chan.basic_get(queue='res',
                                                                 no_ack=True)
                if res and not resource:
                    resource = res.from_dict()
                    self._logger.info('Resource %s received'%resource.uid)

                if workload and resource:

                    sel = Selector()
                    sel.criteria = self._ts_criteria
                    self._selected_tasks = sel.select(collection=self._workload.task_list,
                                                      count=self._workload.num_tasks)
                    self._logger.info('Task selection complete')

                    sel = Selector()
                    sel.criteria = self._rs_criteria
                    self._selected_nodes = sel.select(collection=self._resource.node_list,
                                                      count=self._resource.num_cores)
                    self._logger.info('Resource selection complete')

                    m = Binder()
                    m.criteria = self._b_criteria
                    self._schedule = m.bind(
                        self._selected_tasks, self._selected_nodes)

                    self._logger.info('Binding complete')

                    chan.basic_publish(exchange='executor',
                                       routing_key='schedule',
                                       body=self._schedule)

                    self._logger.info('Schedule published to executor')

                    workload, resource = None, None

                method_frame, header_frame, cfg = chan.basic_get(queue='cfg',
                                                                 no_ack=True)
                if cfg:
                    self._logger.info('Reassigning criteria')
                    self._set_criteria(cfg)



        raise KeyboardInterrupt:

            self._logger.info('Closing %s'%self._uid)


        raise Exception as ex:

            self._logger.exception('WLMS failed with %s'%ex)

        # return {'tte': e.get_tte(), 'util': e.get_util()}
