from calculator.entities.core import Core
from calculator.entities.task import Task

def test_task_init():

    t = Task()
    assert t.uid.split('.')[0] == 'task'
    assert t.ops == 0
    assert t.start_time == None
    assert t.end_time == None
    assert t.exec_core == None

    t = Task(ops=100)
    assert t.ops == 100
    t.ops = 200
    assert t.ops == 200

    t = Task(no_uid=True)
    assert t.uid == None

def test_core_execute():

    c = Core(perf=10)
    t = Task(ops=100)
    c.execute(t)

    assert t.start_time == 0
    assert t.end_time == 10
    assert t.exec_core == c.uid

def test_task_to_dict():

    t = Task(ops=100)
    tmp = t.to_dict()

    assert tmp['uid'] == t.uid
    assert tmp['ops'] == t.ops
    assert tmp['start_time'] == t.start_time
    assert tmp['end_time'] == t.end_time
    assert tmp['exec_core'] == t.exec_core

    c = Core(perf=10)
    t = Task(ops=100)
    c.execute(t)
    tmp = t.to_dict()

    assert tmp['uid'] == t.uid
    assert tmp['ops'] == t.ops
    assert tmp['start_time'] == t.start_time
    assert tmp['end_time'] == t.end_time
    assert tmp['exec_core'] == t.exec_core


def test_task_from_dict():

    tmp = { 'uid': 'task.1234',
            'ops': 10,
            'start_time': 100,
            'end_time': 120,
            'exec_core': 'core.5678'
        }

    t = Task(no_uid=True)
    t.from_dict(tmp)

    assert tmp['uid'] == t.uid
    assert tmp['ops'] == t.ops
    assert tmp['start_time'] == t.start_time
    assert tmp['end_time'] == t.end_time
    assert tmp['exec_core'] == t.exec_core