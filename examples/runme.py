from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint

if __name__ == '__main__':

    # Create a workload with a specific number of tasks with number of
    # operations per task drawn from a distribution
    wl = Workload(  num_tasks=8,        # no. of tasks
                    ops_dist='uniform', # distribution to draw samples from
                    dist_mean=10,       # mean of distribution
                    dist_var=4          # variance of distribution
                )

    # Create a resource with a specific number of cores with performance of each
    # core drawn from a distribution
    res = Resource( num_cores=8,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=3,        # mean of distribution
                    dist_var=1          # variance of distribution
                )

    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria
    eng = Engine(cfg_path='./config_baseline.yml')

    # Run given workload on resources using the configured WLMS
    eng.run(wl, res)
