from calculator.components.algorithms.spatial_binding_algos import round_robin, largest_to_fastest, smallest_to_fastest, random_placer
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

def test_l2f():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = largest_to_fastest(wl.task_list,rs.core_list)    

    tasks = sorted(wl.task_list, key=lambda task:task.ops, reverse=True)
    cores = sorted(rs.core_list, key=lambda unit: unit.perf, reverse=True)

    for ind, x in enumerate(tasks):
        assert schedule[ind] == {'task': x, 
                                 'core': cores[ind%4]}

def test_s2f():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = smallest_to_fastest(wl.task_list,rs.core_list)    

    tasks = sorted(wl.task_list, key=lambda task: task.ops)
    cores = sorted(rs.core_list, key=lambda unit: unit.perf, reverse=True)

    for ind, x in enumerate(tasks):
        assert schedule[ind] == {'task': x, 
                                 'core': cores[ind%len(cores)]}

def test_random_placer():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)
    rs = Resource(num_cores=4, perf_dist='uniform', dist_mean = 5, temporal_var=0, spatial_var=0)
    rs.create_core_list()

    schedule = random_placer(wl.task_list,rs.core_list) 

    assert len(schedule)==wl.num_tasks
    for x in schedule:
        assert set(x.keys()) == set(['task','core'])
        assert None not in x.values()