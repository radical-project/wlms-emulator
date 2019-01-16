from calculator import Workload
from calculator.components.algorithms.temporal_binding_algos import random_order, fastest_first, slowest_first

def test_random_order():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)

    schedule = random_order(workload=wl.task_list)
    assert len(schedule)==wl.num_tasks

def test_fastest_first():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    schedule = fastest_first(wl.task_list)

    assert schedule == sorted(wl.task_list, key=lambda task: task.ops, reverse=True)

def test_smallest_first():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    schedule = fastest_first(wl.task_list)

    assert schedule == sorted(wl.task_list, key=lambda task: task.ops)