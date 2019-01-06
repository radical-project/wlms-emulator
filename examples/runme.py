from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint
import yaml

def get_workload():

    # Create a workload with a specific number of tasks with number of
    # operations per task drawn from a distribution
    wl = Workload(  num_tasks=128,          # no. of tasks
                    ops_dist='uniform',     # distribution to draw samples from
                    dist_mean=1024,         # mean of distribution
                    dist_var=4              # variance of distribution
                )

    return wl


if __name__ == '__main__':

    # Create a resource with a specific number of cores with performance of each
    # core drawn from a distribution
    res = Resource( num_cores=128,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=32,        # mean of distribution
                    temporal_var=1,     # temporal variance of core performance
                    spatial_var=1     # spatial variance of core performance
                )

    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria
    eng = Engine(cfg_path='./config_baseline.yml')

    eng.assign_cfg()
    eng.assign_resource(res)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=0)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=5)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=10)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=15)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=20)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=25)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=30)

    wl = get_workload()
    eng.assign_workload(wl, submit_at=35)
    