import radical.utils as ru
from ..exceptions import *
from .resource import Resource
from .workload import Workload
from yaml import load
import pika
import json

class Engine(object):

    def __init__(self, cfg_path):

        self._resource = None
        self._workload = None
        self._logger = ru.Logger('radical.engine')

        with open(cfg_path, 'r') as stream:
            cfg = load(stream)
            self._parse_cfg(cfg)

    def _parse_cfg(self, cfg):

        self._host = cfg['rmq']['host']
        self._port = cfg['rmq']['port']
        self._tgt_exchange = cfg['rmq']['wlms']['exchange']
        self._logger.info('Configuration parsed')

    def run(self, workload, resource):

        if not isinstance(workload, Workload):
            raise CalcTypeError(expected_type=Workload,
                                actual_type=type(workload)
                            )

        if not isinstance(resource, Resource):
            raise CalcTypeError(expected_type=Resource,
                                actual_type=type(resource)
                            )

        wl_as_dict = workload.to_dict()
        res_as_dict = resource.to_dict()

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port))
        chan = conn.channel()

        chan.basic_publish( body=json.dumps(wl_as_dict),
                            exchange=self._tgt_exchange,
                            routing_key='wl'
                        )

        chan.basic_publish( body=json.dumps(res_as_dict),
                            exchange=self._tgt_exchange,
                            routing_key='res'
                        )

        conn.close()

    def reset_cfg(self, cfg):

        if not isinstance(cfg, dict):
            raise CalcTypeError(expected_type=dict,
                                actual_type=type(cfg)
                            )

        if set(cfg.keys()) != set(['task_selector', 'resource_selector', 'binder']):
            raise ValueError("cfg requires ['task_selector', 'resource_selector', 'binder'] keys")

        if None in cfg.values():
            raise ValueError('cfg keys cannot be None')

        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port))
        chan = conn.channel()

        chan.basic_publish( body=json.dumps(cfg),
                            exchange=self._tgt_exchange,
                            routing_key='cfg'
                        )

        conn.close()