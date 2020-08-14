import radical.utils as ru
from ..exceptions import CalcTypeError
from .resource import Resource
from .workload import Workload
from yaml import load
import pika
import json


class Engine(object):

    def __init__(self, cfg_path):

        self._resource = None
        self._workload = None
        self._uid = ru.generate_id('engine', mode=ru.ID_UNIQUE)
        self._logger = ru.Logger('radical.engine')

        with open(cfg_path, 'r') as stream:
            cfg = load(stream)
            self._parse_cfg(cfg)

    def _parse_cfg(self, cfg):

        self._host = cfg['rmq']['host']
        self._port = cfg['rmq']['port']
        self._id = cfg['rmq']['id']
        self._password = cfg['rmq']['password']
        self._wlms_exchange = cfg['rmq']['wlms']['exchange']
        self._executor_exchange = cfg['rmq']['executor']['exchange']
        self._logger.info('Configuration parsed')

    def assign_resource(self, resource):

        if not isinstance(resource, Resource):
            raise CalcTypeError(expected_type=Resource,
                                actual_type=type(resource)
                                )

        res_as_dict = resource.to_dict()

        credentials = pika.PlainCredentials(self._id, self._password)
        
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))
        chan = conn.channel()

        chan.basic_publish(body=json.dumps(res_as_dict),
                           exchange=self._wlms_exchange,
                           routing_key='res'
                           )

        conn.close()

    def assign_cfg(self):

        cfg_as_dict = {'engine_uid': self._uid}

        credentials = pika.PlainCredentials(self._id, self._password)
        
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))
        chan = conn.channel()

        chan.basic_publish(body=json.dumps(cfg_as_dict),
                           exchange=self._executor_exchange,
                           routing_key='cfg_as'
                           )

        conn.close()

    def assign_workload(self, workload, submit_at):

        if not isinstance(workload, Workload):
            raise CalcTypeError(expected_type=Workload,
                                actual_type=type(workload)
                                )

        wl_as_dict = workload.to_dict()
        wl_as_dict['submit_time'] = submit_at

        credentials = pika.PlainCredentials(self._id, self._password)
        
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._host, port=self._port, credentials = credentials))
        chan = conn.channel()

        chan.basic_publish(body=json.dumps(wl_as_dict),
                           exchange=self._wlms_exchange,
                           routing_key='wl'
                           )

        conn.close()
