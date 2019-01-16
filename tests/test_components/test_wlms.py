from calculator.exceptions import *
from calculator.components.wlms import WLMS
from calculator import Resource, Workload
import pytest
import os
from random import random
from glob import glob
import radical.utils as ru
import yaml
import pika
import json
from time import sleep
import threading


def test_wlms_init():

    fpath = os.path.dirname(os.path.abspath(__file__))
    wlms = WLMS(cfg_path='%s/../config_test.yml'%fpath)

    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)

    assert wlms._uid.split('.')[0] == 'wlms'
    assert wlms._ts_criteria == cfg['criteria']['task_selector']
    assert wlms._rs_criteria == cfg['criteria']['resource_selector']
    assert wlms._sb_criteria == cfg['criteria']['spatial_binder']
    assert wlms._tb_criteria == cfg['criteria']['temporal_binder']
    assert wlms._host == cfg['rmq']['host']
    assert wlms._port == cfg['rmq']['port']


def test_wlms_parse_cfg():

    fpath = os.path.dirname(os.path.abspath(__file__))
    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)
    wlms = WLMS(cfg_path='%s/../config_test.yml'%fpath)
    wlms._parse_cfg(cfg)

    assert wlms._host == cfg['rmq']['host']
    assert wlms._port == cfg['rmq']['port']
    assert wlms._exchange == cfg['rmq']['wlms']['exchange']
    assert wlms._tgt_exchange == cfg['rmq']['executor']['exchange']
    assert wlms._wl_queue == cfg['rmq']['wlms']['queues']['workload']
    assert wlms._res_queue == cfg['rmq']['wlms']['queues']['resource']
    assert wlms._exec_queue == cfg['rmq']['wlms']['queues']['executor']


def test_wlms_set_criteria():

    fpath = os.path.dirname(os.path.abspath(__file__))
    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)

    wlms = WLMS(cfg_path='%s/../config_test.yml'%fpath)
    wlms._set_criteria(cfg['criteria'])

    assert wlms._ts_criteria == cfg['criteria']['task_selector']
    assert wlms._rs_criteria == cfg['criteria']['resource_selector']
    assert wlms._sb_criteria == cfg['criteria']['spatial_binder']
    assert wlms._tb_criteria == cfg['criteria']['temporal_binder']


def func_for_test_wlms_run(wlms):
    
    try:
        wlms.run()
        
    except Exception as ex:
        print 'Thread failed with %s'%ex

def test_wlms_run():
    
    fpath = os.path.dirname(os.path.abspath(__file__))
    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)

    wlms = WLMS(cfg_path='%s/../config_test.yml'%fpath)
    t = threading.Thread(target=func_for_test_wlms_run, args=(wlms,))
    t.daemon= True
    t.start()

    conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg['rmq']['host'], port=cfg['rmq']['port']))
    chan = conn.channel()

    res = Resource(num_cores=10)
    res_as_dict = res.to_dict()
    chan.basic_publish( body=json.dumps(res_as_dict),
                        exchange=cfg['rmq']['wlms']['exchange'],
                        routing_key='res')

    wl = Workload(num_tasks=10)
    wl_as_dict = wl.to_dict()
    wl_as_dict['submit_time'] = 5
    chan.basic_publish( body=json.dumps(wl_as_dict),
                        exchange=cfg['rmq']['wlms']['exchange'],
                        routing_key='wl')

    schedule = None
    while not schedule:
        method_frame, header_frame, schedule = chan.basic_get(queue=cfg['rmq']['executor']['queues']['schedule'], 
                                                            no_ack=True)

    schedule = json.loads(schedule)
    assert len(schedule)==10

    t.join(timeout=5)