def round_robin(workload, resource):

    m = dict()
    for idx, task in enumerate(workload):
        m[task.uid] = resource[idx%len(resource)].uid

    return m

def optimize_tte(workload, resource):
    pass

def optimize_util(workload, resource):
    pass