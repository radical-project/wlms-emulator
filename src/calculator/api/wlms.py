import radical.utils as ru
from ..components.selector import Selector
from ..components.binder import Binder
from ..components.executor import Executor
from ..exceptions import *

class WLMS(object):

    def __init__(   self,
                    workload,
                    resource,
                    task_selection_criteria='all',
                    resource_selection_criteria='all',
                    binding_criteria='tte'):

        self._uid = ru.generate_id('engine')
        self._workload = workload
        self._resource = resource
        self._schedule = list()

        valid_ts_criteria = ['all']
        valid_rs_criteria = ['all']
        valid_b_criteria = ['rr','tte', 'util']

        if task_selection_criteria not in valid_ts_criteria or \
            resource_selection_criteria not in valid_rs_criteria or \
            binding_criteria not in valid_b_criteria:
                raise CalcError('%s, %s, or %s is invalid'%(task_selection_criteria,
                                                            resource_selection_criteria,
                                                            binding_criteria))

        self._ts_criteria = task_selection_criteria
        self._rs_criteria = resource_selection_criteria
        self._b_criteria = binding_criteria

    def run(self):

        sel = Selector()
        sel.criteria = self._ts_criteria
        self._selected_tasks = sel.select(collection=self._workload.task_list, count=self._workload.num_tasks)
        sel = Selector()
        sel.criteria = self._rs_criteria
        self._selected_nodes = sel.select(collection=self._resource.node_list, count=self._resource.num_cores)

        m = Binder()
        m.criteria = self._b_criteria
        self._schedule = m.bind(self._selected_tasks, self._selected_nodes)

        e = Executor(schedule=self._schedule)
        e.execute()
        # return {'tte': e.get_tte(), 'util': e.get_util()}

    def generate_profile(self):

        for task in self._selected_tasks:
            print 'Task: %s | Core: %s | Start time: %.2fs | End time: %.2fs | Execution time: %.2fs' % (
                task.uid, task.exec_node, task.start_time, task.end_time, task.end_time - task.start_time)
