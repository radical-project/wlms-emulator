from calculator.components.algorithms.selection_algos import select_all
from calculator import Workload

def test_select_all():
    
    wl = Workload(num_tasks=10, ops_dist='uniform', dist_mean=10, dist_var=0)

    assert len(select_all(wl.task_list, 5)) == wl.num_tasks