import radical.utils as ru
from ..components.selector import Selector
from ..components.binder import Binder
from ..components.executor import Executor
from ..exceptions import *

class Engine(object):

    def __init__(   self,
                    workload,
                    resource,
                    task_selection_criteria=None,
                    resource_selection_criteria=None,
                    binding_criteria='tte'):

        self._uid = ru.generate_id('engine')
        self._workload = workload
        self._resource = resource
        self._schedule = list()

        valid_ts_criteria = [None]
        valid_rs_criteria = [None]
        valid_b_criteria = [None, 'tte']

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

        sel = Selector(self._ts_criteria)
        self._selected_tasks = sel.select(entity=self._workload.workload, size=len(self._workload.workload))
        sel = Selector(self._rs_criteria)
        self._selected_nodes = sel.select(entity=self._resource.resource, size=len(self._resource.resource))

        m = Binder(criteria=self._binder_criteria)
        self._schedule = m.bind(self._selected_wl, self._selected_res)

        e = Executor(schedule=self._schedule)
        # return {'tte': e.get_tte(), 'util': e.get_util()}

    def get_tte(self):
        maxx = 0
        minn = 1000000000000
        for t in self._selected_wl:
            if t.start_time < minn:
                minn = t.start_time
            if t.end_time > maxx:
                maxx = t.end_time

        return maxx-minn

    def get_util(self):

        utilization = 0.0
        for unit in self._selected_res:
            for util_block in unit.util:
                utilization += (util_block[1] - util_block[0])

            #print 'Unit: %s, Util: %s' %(unit.uid, unit.util)

        return utilization

    def print_profile(self):

        for t in self._selected_res:
            print 'Task: %s | Unit: %s | Start: %s | End: %s | Exec: %s' % (
                t.uid, t.exec_unit, t.start_time, t.end_time, t.end_time - t.start_time)
