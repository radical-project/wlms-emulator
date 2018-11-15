from calculator.api.workload import Workload
from calculator.exceptions import *
from calculator.entities.task import Task

def test_workload_init():

    wl = Workload(num_tasks=1, dist_value=10, dist_var=2)

    assert wl._uid.split('.')[0] == 'workload'
    assert wl._dist_candidates == ['uniform', 'normal']
    assert isinstance(wl.task_list,list) and len(wl.task_list)==1
    assert wl.num_tasks == 1
    assert wl._ops_dist == 'uniform'
    assert wl._dist_value == 10
    assert wl._dist_var == 2

def test_workload_create():

    wl = Workload(num_tasks=10, dist_value=100, dist_var=10)
    assert len(wl._task_list) == 10
    for t in wl._task_list:
        assert isinstance(t, Task)

def test_workload_reset():
    wl = Workload(num_tasks=1, dist_value=10, dist_var=2)
    wl._task_list[0].start_time = 100
    wl._task_list[0].end_time = 120
    wl._task_list[0].exec_node = 'node.0000'

    assert wl._task_list[0].start_time == 100
    assert wl._task_list[0].end_time == 120
    assert wl._task_list[0].exec_node == 'node.0000'

    wl.reset()

    assert wl._task_list[0].start_time == None
    assert wl._task_list[0].end_time == None
    assert wl._task_list[0].exec_node == None