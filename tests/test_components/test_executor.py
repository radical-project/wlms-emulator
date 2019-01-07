from calculator.exceptions import *
from calculator.components.executor import Executor
from calculator.entities.task import Task
from calculator.entities.core import Core
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


def test_executor_init():

    fpath = os.path.dirname(os.path.abspath(__file__))
    executor = Executor(cfg_path='%s/../config_test.yml'%fpath)
    assert executor._uid.split('.')[0] == 'executor'
    assert executor._schedule == None
    assert isinstance(executor._profile, dict)
    assert executor._engine_uid == None

def test_executor_parse_cfg():

    cfg = {
            'rmq': {
                    'host': 'two.radical-project.org',
                    'port': 33202,
                    'executor': {
                                    'exchange': 'exch',
                                    'queues': {
                                        'schedule': 'schedule',
                                        'config': './config_test.yml'
                                        },
                                    'profile_loc': './'
                        }
                }
        }

    

    fpath = os.path.dirname(os.path.abspath(__file__))
    cfg_path='%s/../config_test.yml'%fpath
    with open(cfg_path, 'r') as stream:
        cfg = yaml.load(stream)

    executor = Executor(cfg_path=cfg_path)
    executor._parse_cfg(cfg)

    assert executor._host == cfg['rmq']['host']
    assert executor._port == cfg['rmq']['port']
    assert executor._exchange == cfg['rmq']['executor']['exchange']
    assert executor._queue_schedule == cfg['rmq']['executor']['queues']['schedule']
    assert executor._queue_cfg == cfg['rmq']['executor']['queues']['config']
    assert executor._profile_loc == cfg['rmq']['executor']['profile_loc']


def test_executor_record_profile():

    fpath = os.path.dirname(os.path.abspath(__file__))
    executor = Executor(cfg_path='%s/../config_test.yml'%fpath)

    tasks = list()
    engine_uid = 'engine.0000'
    output = list()
    for x in range(10):
        task = Task()
        task.exec_core = 'core.%s'%x
        task.start_time = random()
        task.end_time = random()

        output.append({
                        'task': task.uid,
                        'core': task.exec_core,
                        'ops': task.ops,
                        'start_time': task.start_time,
                        'end_time': task.end_time,
                        'exec_time': task.end_time - task.start_time
                        }
                    )

        tasks.append(task)

    executor._record_profile(tasks, engine_uid)

    assert engine_uid in executor._profile.keys()
    assert output == executor._profile[engine_uid]

    for f in glob('test.*'):
        os.remove(f)


def test_executor_write_profile():

    fpath = os.path.dirname(os.path.abspath(__file__))
    executor = Executor(cfg_path='%s/../config_test.yml'%fpath)

    tasks = list()
    engine_uid = 'engine.0000'
    output = list()
    for x in range(10):
        task = Task()
        task.exec_core = 'core.%s'%x
        task.start_time = random()
        task.end_time = random()

        output.append({
                        'task': task.uid,
                        'ops': task.ops,
                        'core': task.exec_core,
                        'start_time': task.start_time,
                        'end_time': task.end_time,
                        'exec_time': task.end_time - task.start_time
                        }
                    )

        tasks.append(task)

    executor._profile[engine_uid] = output
    executor._profile_loc = '%s/test.prof'%fpath

    executor._write_profile()

    assert os.path.isfile('%s/test.%s.prof'%(fpath,executor._uid))
    prof = ru.read_json('%s/test.%s.prof'%(fpath,executor._uid))

    assert engine_uid in prof.keys()
    assert output == prof[engine_uid]

    for f in glob('test.*'):
        os.remove(f)


def func_for_test_executor_run(executor):

    try:
        executor.run()
        
    except Exception as ex:
        print 'Thread failed with %s'%ex


def test_executor_run():

    fpath = os.path.dirname(os.path.abspath(__file__))

    schedule = list()

    for x in range(10):
        t = Task(ops=100)
        c = Core(10)
        schedule.append({'task': t.to_dict(), 'core':c.to_dict()})

    with open('%s/../config_test.yml'%fpath) as fp:
        cfg = yaml.load(fp)

    conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg['rmq']['host'], port=cfg['rmq']['port']))
    chan = conn.channel()

    chan.basic_publish( exchange=cfg['rmq']['executor']['exchange'],
                        routing_key=cfg['rmq']['executor']['queues']['config'],
                        body=json.dumps({'engine_uid': 'test.0000'}))

    chan.basic_publish( exchange=cfg['rmq']['executor']['exchange'],
                        routing_key=cfg['rmq']['executor']['queues']['schedule'],
                        body=json.dumps(schedule))

    conn.close()  


    executor = Executor(cfg_path='%s/../config_test.yml'%fpath)        
    t = threading.Thread(target=func_for_test_executor_run, args=(executor,))
    t.daemon= True
    t.start()
    t.join(timeout=5)

    executor._write_profile()

    assert os.path.isfile('./profile.%s.json'%(executor._uid))
    prof = ru.read_json('./profile.%s.json'%(executor._uid))

    assert 'test.0000' in prof.keys()
    assert len(prof['test.0000']) == 10

    for ind, x in enumerate(prof['test.0000']):
        assert x['task'] == schedule[ind]['task']['uid']
        assert x['core'] == schedule[ind]['core']['uid']
        assert x['end_time'] == 10 
        assert x['exec_time'] == 10 
        assert x['start_time'] == 0

    for f in glob('profile.*'):
        os.remove(f)