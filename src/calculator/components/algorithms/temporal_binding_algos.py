def fastest_first(workload):
    workload = sorted(workload, key=lambda task: task.ops, reverse=True)
    return workload

def slowest_first(workload):
    
    workload = sorted(workload, key=lambda task: task.ops)
    return workload

def random_order(workload):
    
    from random import shuffle
    shuffle(workload)
    return workload