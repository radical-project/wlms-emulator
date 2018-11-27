from calculator import Workload
from calculator import Resource
from calculator import WLMS
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
    res = Resource( num_cores=5,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=3,        # mean of distribution
                    dist_var=1          # variance of distribution
                )

    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria
    eng = WLMS( workload=wl,                        # workload
                resource=res,                       # resource
                task_selection_criteria='all',      # task selection criteria -- currently selects all tasks
                resource_selection_criteria='all',  # resource selection criteria -- currently selects all cores
                binding_criteria='tte'              # binding criteria -- currently places task with largest number of
                                                    # operations on core with maximum performance
                )

    # Run given workload on resources using the configured WLMS
    eng.run()

    # Print execution profile
    eng.generate_profile()
