from calculator.exceptions import CalcTypeError, CalcValueError, CalcError
from calculator import Engine, Resource, Workload
import pytest
import os
from glob import glob
import radical.utils as ru
import yaml
import pika
import json


def test_engine_init():

    fpath = os.path.dirname(os.path.abspath(__file__))
    engine = Engine(cfg_path='%s/../config_test.yml'%fpath)
    assert engine._uid.split('.')[0] == 'engine'
    assert engine._resource == None
    assert engine._workload == None

def test_engine_parse_cfg():

    cfg = {
            'rmq': {
                    'host': 'two.radical-project.org',
                    'port': 33251,
                    'wlms': {'exchange': 'wlms'},
                    'executor': {'exchange': 'executor'}
                }
        }

    fpath = os.path.dirname(os.path.abspath(__file__))
    engine = Engine(cfg_path='%s/../config_test.yml'%fpath)
    engine._parse_cfg(cfg)

    assert engine._host == cfg['rmq']['host']
    assert engine._port == cfg['rmq']['port']
    assert engine._wlms_exchange == cfg['rmq']['wlms']['exchange']
    assert engine._executor_exchange == cfg['rmq']['executor']['exchange']


def func_setup_mqs(cfg, executor=False):

    conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg['rmq']['host'], port=cfg['rmq']['port']))
    chan = conn.channel()

    chan.exchange_declare(exchange=cfg['rmq']['executor']['exchange'], exchange_type='direct')
    chan.queue_declare(queue=cfg['rmq']['executor']['queues']['config'])
    chan.queue_bind(queue=cfg['rmq']['executor']['queues']['config'], 
                    exchange=cfg['rmq']['executor']['exchange'], 
                    routing_key='cfg')

    chan.exchange_declare(exchange=cfg['rmq']['wlms']['exchange'], exchange_type='direct')
    chan.queue_declare(queue=cfg['rmq']['wlms']['queues']['workload'])
    chan.queue_declare(queue=cfg['rmq']['wlms']['queues']['resource'])
    chan.queue_declare(queue=cfg['rmq']['wlms']['queues']['executor'])

    chan.queue_bind(queue=cfg['rmq']['wlms']['queues']['workload'],
                    exchange=cfg['rmq']['wlms']['exchange'], 
                    routing_key='wl')

    chan.queue_bind(queue=cfg['rmq']['wlms']['queues']['resource'],
                    exchange=cfg['rmq']['wlms']['exchange'], 
                    routing_key='res')

    chan.queue_bind(queue=cfg['rmq']['wlms']['queues']['executor'],
                    exchange=cfg['rmq']['wlms']['exchange'], 
                    routing_key='exec')

    return conn, chan

def test_assign_resource():

    fpath = os.path.dirname(os.path.abspath(__file__))
    engine = Engine(cfg_path='%s/../config_test.yml'%fpath)
    
    with pytest.raises(CalcTypeError):
        engine.assign_resource([])

    rs1 = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs1.create_core_list()

    engine.assign_resource(rs1)

    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)

    
    conn, chan = func_setup_mqs(cfg)
    response = None
    while not response:
        method_frame, header_frame, response = chan.basic_get(queue=cfg['rmq']['wlms']['queues']['resource'], no_ack=True)

    conn.close()

    rs2 = Resource(no_uid=False)
    rs2.from_dict(json.loads(response))

    assert len(rs1.core_list) == len(rs2.core_list)
    for ind, core in enumerate(rs1.core_list):
        assert core.perf == rs2.core_list[ind].perf


def test_assign_cfg():

    fpath = os.path.dirname(os.path.abspath(__file__))
    engine = Engine(cfg_path='%s/../config_test.yml'%fpath)
    engine.assign_cfg()

    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)
    
    conn, chan = func_setup_mqs(cfg)
    response = None
    while not response:
        method_frame, header_frame, response = chan.basic_get(queue=cfg['rmq']['executor']['queues']['config'], no_ack=True)

    response = json.loads(response)
    assert response['engine_uid'] == engine._uid


def test_assign_workload():
    
    fpath = os.path.dirname(os.path.abspath(__file__))
    engine = Engine(cfg_path='%s/../config_test.yml'%fpath)
    
    with pytest.raises(CalcTypeError):
        engine.assign_resource([])

    wl1 = Workload(num_tasks=4, ops_dist='uniform', dist_mean = 5, dist_var=0)
    engine.assign_workload(wl1, submit_at=10)

    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)
    
    conn, chan = func_setup_mqs(cfg)
    response = None
    while not response:
        method_frame, header_frame, response = chan.basic_get(queue=cfg['rmq']['wlms']['queues']['workload'], no_ack=True)

    conn.close()

    response = json.loads(response)
    assert response.pop('submit_time') == 10
    
    wl2 = Workload(no_uid=False)
    wl2.from_dict(response)
    
    assert len(wl1.task_list) == len(wl2.task_list)
    for ind, task in enumerate(wl1.task_list):
        assert task.ops == wl2.task_list[ind].ops