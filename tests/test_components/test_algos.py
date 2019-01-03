from calculator.components.algorithms.binding_algos import round_robin, optimize_tte, optimize_util, random_placer
from calculator.components.algorithms.selection_algos import *
from calculator import Workload
from calculator import Resource


def test_round_robin():
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = round_robin(wl.task_list,rs.core_list)    
    for ind, task in enumerate(wl.task_list):
        assert schedule[ind] == {'task': task, 
                                 'core': rs.core_list[ind%4]}

def test_optimize_tte():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = optimize_tte(wl.task_list,rs.core_list)    

    tasks = sorted(wl.task_list, key=lambda task:task.ops, reverse=True)
    cores = sorted(rs.core_list, key=lambda unit: unit.perf, reverse=True)

    for ind, x in enumerate(tasks):
        assert schedule[ind] == {'task': x, 
                                 'core': cores[ind%4]}

def test_optimize_util():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = optimize_util(wl.task_list,rs.core_list)    

    cores = sorted(rs.core_list, key=lambda unit: unit.perf, reverse=True)

    for ind, x in enumerate(wl.task_list):
        assert schedule[ind] == {'task': x, 
                                 'core': cores[0]}

def test_random_placer():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = random_placer(wl.task_list,rs.core_list) 

    assert len(schedule)==wl.num_tasks
    for x in schedule:
        assert set(x.keys()) == set(['task','core'])
        assert None not in x.values()

def test_select_all():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)

    assert len(select_all(wl.task_list, 5)) == wl.num_tasks