from calculator.entities.core import Core
from calculator.entities.task import Task
from calculator.exceptions import *
import pytest

def test_core_init():

    c = Core()
    assert c.uid.split('.')[0] == 'core'
    assert c.perf == 0
    assert isinstance(c.util, list) and len(c.util)==0
    assert isinstance(c.task_history, list) and len(c.task_history)==0

    c = Core(perf=10)
    assert c.perf == 10
    c.perf = 20
    assert c.perf == 20

    c = Core(no_uid=True)
    assert c.uid == None

def test_core_execute():

    c = Core(perf=10)
    t = Task(ops=100)
    c.execute(t)

    assert c.util == [[t.start_time, t.end_time]]
    assert c.task_history == [t.uid]

def test_core_to_dict():

    c = Core(perf=10)
    tmp = c.to_dict()

    assert tmp['uid'] == c.uid
    assert tmp['perf'] == c.perf
    assert tmp['util'] == c.util
    assert tmp['task_history'] == c.task_history

    c = Core(perf=10)
    t = Task(ops=100)
    c.execute(t)
    tmp = c.to_dict()

    assert tmp['uid'] == c.uid
    assert tmp['perf'] == c.perf
    assert tmp['util'] == c.util
    assert tmp['task_history'] == c.task_history


def test_core_from_dict():

    tmp = { 'uid': 'core.1234',
            'perf': 10,
            'util': [[3,4]],
            'task_history': ['task.5678']
        }

    c = Core(no_uid=True)
    c.from_dict(tmp)

    assert tmp['uid'] == c.uid
    assert tmp['perf'] == c.perf
    assert tmp['util'] == c.util
    assert tmp['task_history'] == c.task_history