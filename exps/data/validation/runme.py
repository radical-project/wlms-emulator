from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint

def get_workload():

    # Create a workload with a specific number of tasks with number of
    # operations per task drawn from a distribution
    wl = Workload(  num_tasks=4,          # no. of tasks
                    ops_dist='uniform',     # distribution to draw samples from
                    dist_mean=1024,         # mean of distribution
                    dist_var=0              # variance of distribution
                )

    return wl

if __name__ == '__main__':

    # Create a resource with a specific number of cores with performance of each
    # core drawn from a distribution
    res = Resource( num_cores=4,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=32,        # mean of distribution
                    temporal_var=0,     # temporal variance of core performance
                    spatial_var=0     # spatial variance of core performance
                )

    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria
    eng = Engine(cfg_path='./config_l2f.yml')

    eng.assign_cfg()
    eng.assign_resource(res)

    wl1 = get_workload()
    eng.assign_workload(wl1, submit_at=0)

    wl2 = get_workload()
    eng.assign_workload(wl2, submit_at=5)
    