def round_robin(workload, resource):

    m = list()
    for idx, task in enumerate(workload):
        m.append({'task': task,
                  'core': resource[idx % len(resource)]})

    return m


def largest_to_fastest(workload, resource):

    m = list()

    workload = sorted(workload, key=lambda task: task.ops, reverse=True)
    resource = sorted(resource, key=lambda unit: unit.perf, reverse=True)

    for idx, task in enumerate(workload):
        m.append({'task': task,
                  'core': resource[idx % len(resource)]})

    return m

def smallest_to_fastest(workload, resource):

    m = list()

    workload = sorted(workload, key=lambda task: task.ops)
    resource = sorted(resource, key=lambda unit: unit.perf, reverse=True)

    for idx, task in enumerate(workload):
        m.append({'task': task,
                  'core': resource[idx % len(resource)]})

    return m

def random_placer(workload, resource):

    from random import shuffle

    m = list()
    shuffle(workload)
    shuffle(resource)

    for idx, task in enumerate(workload):
        m.append({'task': task,
                  'core': resource[idx % len(resource)]})

    return m
