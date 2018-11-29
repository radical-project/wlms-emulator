from calculator.api.workload import Workload
from calculator.exceptions import *
from calculator.entities.task import Task

def test_workload_init():

    wl = Workload()

    assert wl._uid.split('.')[0] == 'workload'
    assert wl._dist_options == ['uniform', 'normal']
    assert isinstance(wl.task_list,list) and len(wl.task_list)==1
    assert wl.num_tasks == 1
    assert wl._ops_dist == 'uniform'
    assert wl._dist_mean == 10
    assert wl._dist_var == 0

    wl = Workload(no_uid=True)
    assert wl._uid == None

def test_workload_create():

    wl = Workload(num_tasks=10, dist_mean=100, dist_var=10)
    assert len(wl._task_list) == 10
    for t in wl._task_list:
        assert isinstance(t, Task)

def test_workload_reset():
    wl = Workload()
    wl._task_list[0].start_time = 100
    wl._task_list[0].end_time = 120
    wl._task_list[0].exec_core = 'core.0000'

    assert wl._task_list[0].start_time == 100
    assert wl._task_list[0].end_time == 120
    assert wl._task_list[0].exec_core == 'core.0000'

    wl.reset()

    assert wl._task_list[0].start_time == None
    assert wl._task_list[0].end_time == None
    assert wl._task_list[0].exec_core == None


def test_workload_to_dict():

    wl = Workload()
    tmp = wl.to_dict()

    assert tmp['uid'] == wl._uid
    assert tmp['num_tasks'] == wl.num_tasks
    assert tmp['ops_dist'] == wl._ops_dist
    assert tmp['dist_mean'] == wl._dist_mean
    assert tmp['dist_var'] == wl._dist_var

    tmp_list = list()
    for task in wl.task_list:
        tmp_list.append(task.to_dict())

    assert tmp_list == tmp['task_list']


    wl = Workload(num_tasks=10, dist_mean=100, dist_var=10)
    tmp = wl.to_dict()

    assert tmp['uid'] == wl._uid
    assert tmp['num_tasks'] == wl.num_tasks
    assert tmp['ops_dist'] == wl._ops_dist
    assert tmp['dist_mean'] == wl._dist_mean
    assert tmp['dist_var'] == wl._dist_var

    tmp_list = list()
    for task in wl.task_list:
        tmp_list.append(task.to_dict())

    assert tmp_list == tmp['task_list']

def test_resource_from_dict():

    tmp = {
            'uid': 'resource.1234',
            'num_tasks': 10,
            'ops_dist': 'uniform',
            'dist_mean': 20,
            'dist_var': 5,
            'task_list': list()
        }

    for x in range(tmp['num_tasks']):
        tmp['task_list'].append(Task(ops=20).to_dict())

    wl = Workload(no_uid=True)
    wl.from_dict(tmp)

    assert tmp['uid'] == wl._uid
    assert tmp['num_tasks'] == wl.num_tasks
    assert tmp['ops_dist'] == wl._ops_dist
    assert tmp['dist_mean'] == wl._dist_mean
    assert tmp['dist_var'] == wl._dist_var

    tmp_list = list()
    for task in wl.task_list:
        tmp_list.append(task.to_dict())

    assert tmp_list == tmp['task_list']