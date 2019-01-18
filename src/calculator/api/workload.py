import radical.utils as ru
import numpy as np
from ..entities.task import Task


class Workload(object):

    def __init__(self, num_tasks=0, ops_dist='uniform', dist_mean=10, dist_var=0, no_uid=False):

        # Initialize

        self._uid = None
        self._dist_options = ['uniform', 'normal']
        self._task_list = list()

        if ops_dist not in self._dist_options:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_options)))

        self._num_tasks = num_tasks
        self._ops_dist = ops_dist
        self._dist_mean = dist_mean
        self._dist_var = dist_var

        if not no_uid:
            self._uid = ru.generate_id('workload')
            self._create_task_list()

    @property
    def uid(self):
        return self._uid

    @property
    def num_tasks(self):
        return self._num_tasks

    @property
    def task_list(self):
        return self._task_list

    def _create_task_list(self):

        # Select N samples from the selected distribution
        if self._ops_dist == 'uniform':
            self._samples = list(np.random.uniform(low=self._dist_mean - self._dist_var,
                                              high=self._dist_mean + self._dist_var,
                                              size=self._num_tasks))
        elif self._ops_dist == 'normal':
            if self._dist_var:
                self._samples = list(np.random.normal(self._dist_mean, self._dist_var, self._num_tasks))
            else:
                self._samples = [self._dist_mean for _ in range(self._num_tasks)]

        # Create N tasks with the selected samples
        self._task_list = [Task(self._samples[i])
                           for i in range(self._num_tasks)]

    def to_dict(self):

        task_list_as_dict = list()
        for task in self._task_list:
            task_list_as_dict.append(task.to_dict())

        return {
            'uid': self._uid,
            'num_tasks': self._num_tasks,
            'ops_dist': self._ops_dist,
            'dist_mean': self._dist_mean,
            'dist_var': self._dist_var,
            'task_list': task_list_as_dict
        }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._num_tasks = entry['num_tasks']
        self._ops_dist = entry['ops_dist']
        self._dist_mean = entry['dist_mean']
        self._dist_var = entry['dist_var']

        for task in entry['task_list']:
            t = Task(no_uid=True)
            t.from_dict(task)
            self._task_list.append(t)

    def reset(self):

        for t in self._task_list:
            t.start_time = None
            t.end_time = None
            t.exec_core = None
