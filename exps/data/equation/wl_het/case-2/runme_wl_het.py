from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint
import yaml
from time import sleep
from glob import glob
import os
import radical.utils as ru
import shutil
import argparse

def get_workload(mean, var, tasks=128):

    # Create a workload with a specific number of tasks with number of
    # operations per task drawn from a distribution
    wl = Workload(  num_tasks=tasks,          # no. of tasks
                    ops_dist='uniform',     # distribution to draw samples from
                    dist_mean=mean,         # mean of distribution
                    dist_var=var             # variance of distribution
                )

    return wl


def get_resource(mean, t_var, s_var, cores=128):

    # Create a resource with a specific number of cores with performance of each
    # core drawn from a distribution
    res = Resource( num_cores=cores,        # no.
                    perf_dist='uniform',# distribution to draw samples from
                    dist_mean=mean,        # mean of distribution
                    temporal_var=t_var,     # temporal variance of core performance
                    spatial_var=s_var     # spatial variance of core performance
                )

    return res


def get_engine(res):

    eng = Engine(cfg_path='./config.yml')
    eng.start_comps()
    eng.assign_cfg()
    eng.assign_resource(res)
    return eng


def store_data(store, src):

    new_data = ru.read_json(src)
    store.update(new_data)

if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('--wl_mean', type=int)
    parser.add_argument('--wl_spread', type=float)
    parser.add_argument('--res_mean', type=int)
    parser.add_argument('--res_het', type=float)
    parser.add_argument('--res_dyn', type=float)
    parser.add_argument('--conc', type=int)
    parser.add_argument('--gens', type=int)
    parser.add_argument('--trials', type=int)

    args = parser.parse_args()
    wl_mean = args.wl_mean
    wl_spread = args.wl_spread
    res_mean = args.res_mean
    res_het = args.res_het
    res_dyn = args.res_dyn
    conc = args.conc
    gens = args.gens
    trials = args.trials

    with open('./config.yml','r') as fp:
        cfg = yaml.load(fp)

    early = cfg['wlms']['early_binding']
    
    # Create WLMS instance with a workload, resource, selection criteria, and
    # binding criteria

    store = {}

    for tr in range(trials):

        t = 0
        res = get_resource(mean=res_mean, t_var=res_dyn*res_mean/100, s_var=res_het*res_mean/100, cores=conc)
        eng = get_engine(res)

        if early:
            wl = get_workload(mean=wl_mean, var=wl_spread*wl_mean/100, tasks=conc*gens)
            eng.assign_workload(wl, submit_at=t)
            sleep(5)

        else:
            for _ in range(gens):

                wl = get_workload(mean=wl_mean, var=wl_spread*wl_mean/100, tasks=conc)
                eng.assign_workload(wl, submit_at=t)
                t += 5
        
            sleep(5)

        eng.stop_comps()

        profs = glob('./profile.*.json')
        assert len(profs) == 1
        store_data(store, profs[0])
        os.remove(profs[0])


    ru.write_json(store, './prof_var_%s.json'%wl_spread)
    if not os.path.isdir('mean-%s-%s-gens-%s-conc-%s'%(wl_mean, res_mean, gens, conc)):
        os.makedirs('mean-%s-%s-gens-%s-conc-%s'%(wl_mean, res_mean, gens, conc))
    shutil.move('./prof_var_%s.json'%wl_spread,'mean-%s-%s-gens-%s-conc-%s'%(wl_mean,res_mean, gens, conc))
