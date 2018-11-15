import radical.utils as ru
import numpy as np
from ..entities.task import Task
from ..exceptions import *

class Workload(object):

    def __init__(self, num_tasks=0, ops_dist='uniform', dist_value=None, dist_var=None):

        # Initialize
        self._uid = ru.generate_id('workload')
        self._dist_candidates = ['uniform', 'normal']
        self._task_list = list()

        if not num_tasks or not ops_dist or not dist_value or not dist_var:
            raise CalcMissingError(obj=self._uid,
                                   missing_attribute='num_tasks, ops_dist, dist_value, or dist_var')

        if ops_dist not in self._dist_candidates:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_candidates)))

        self._num_tasks = num_tasks
        self._ops_dist = ops_dist
        self._dist_value = dist_value
        self._dist_var = dist_var

        self._create_task_list()

    @property
    def num_tasks(self):
        return self._num_tasks

    @property
    def task_list(self):
        return self._task_list

    def _create_task_list(self):

        # Select N samples from the selected distribution
        if self._ops_dist == 'uniform':
            self._samples = np.random.uniform(low=self._dist_value - self._dist_var,
                                              high=self._dist_value + self._dist_var,
                                              size=self._num_tasks)
        elif self._ops_dist == 'normal':
            self._samples = [
                self._dist_value*np.random.randn()+self._dist_var for _ in range(self._num_tasks)]

        # Create N tasks with the selected samples
        self._task_list = [Task(self._samples[i])
                           for i in range(self._num_tasks)]

    def reset(self):

        for t in self._task_list:
            t.start_time = None
            t.end_time = None
            t.exec_node = None
