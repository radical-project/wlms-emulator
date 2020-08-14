from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint
import yaml
from time import sleep


def get_workload(mean, var, tasks=128):

    # Create a workload with a specific number of tasks with number of
    # operations per task drawn from a distribution
    wl = Workload(  num_tasks=tasks,          # no. of tasks
                    ops_dist='uniform',     # distribution to draw samples from
                    dist_mean=mean,         # mean of distribution
                    dist_var=var             # variance of distribution
                )

    return wl


def get_resource(mean, t_var, s_var, cores=128): #, num_gen = 2):

    # Create a resource with a specific number of cores with performance of each
    # core drawn from a distribution
    res = Resource( num_cores=cores,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=mean,        # mean of distribution
                    temporal_var=t_var,     # temporal variance of core performance
                    spatial_var=s_var     # spatial variance of core performance
                    #num_gen = num_gen
                )

    return res


def get_engine(res):

    eng = Engine(cfg_path='./config.yml')
    eng.assign_cfg()
    eng.assign_resource(res)
    return eng


if __name__ == '__main__':

    trials=1
    
    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria
    for var in [2]:

        for _ in range(trials):

            t = 0
            res = get_resource(mean=32, t_var=var, s_var=0, cores=4)
            eng = get_engine(res)

            wl = get_workload(mean=1024, var=var*32, tasks=8)
            eng.assign_workload(wl, submit_at=t)

            sleep(5)
