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

def largest_to_first_available(schedule_as_dict, cores_as_dict):
    core_list = list()
    task_list = list()
    schedule_edit_dict = list()
    for s in schedule_as_dict:
        core = cores_as_dict[s['core']['uid']]
        task_list.append(s['task'])
        core_list.append([core, core._util[-1][1]])
    for i in range(len(core_list)):
        core_list[i][0].perf = schedule_as_dict[i]['core']['perf']
    core_list = sorted(core_list, key=lambda l:l[1])

    for i in range(len(core_list)):
        tmp = {'task': task_list[i], 'core': core_list[i][0].to_dict()}
        schedule_edit_dict.append(tmp)
    return schedule_edit_dict

def largest_to_fastest_alt(workload, resource):
    
    m = list()
    
    resource = sorted(resource, key=lambda unit: unit.perf, reverse=True)
    workload = sorted(workload, key=lambda task: task.ops, reverse=True)
    
    len_wl = len(workload)
    len_res = len(resource)
    num_gen = len_wl / len_res
    
    for i in range(num_gen/2):
        if i % 2 == 0:
            gen_id = 2*i + 1
            offset = gen_id * len_res
            workload[offset:offset+len_res] = sorted(workload[offset:offset+len_res], key=lambda task: task.ops)

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
