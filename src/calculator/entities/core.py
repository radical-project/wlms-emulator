import radical.utils as ru
import numpy as np


class Core(object):

    def __init__(self, perf=0, no_uid=False):

        self._uid = None
        if not no_uid:
            self._uid = ru.generate_id('core', mode=ru.ID_PRIVATE)
        self._perf = perf
        self._util = list()
        self._task_history = list()

    @property
    def uid(self):
        return self._uid

    @property
    def perf(self):
        return self._perf

    @property
    def util(self):
        return self._util

    @property
    def task_history(self):
        return self._task_history

    @perf.setter
    def perf(self, val):
        self._perf = val

    @util.setter
    def util(self, val):
        self._util = val

    @task_history.setter
    def task_history(self, val):
        self._task_history = val

    def refresh_perf(self, dist_val, mean_val, temp_var):
        if dist_val == 'uniform':
            self._perf = np.random.uniform(low = mean_val - temp_var, high = mean_val + temp_var, size=1)[0]
        elif dist_val == 'normal':
            self._perf = np.random.normal(mean_val, temp_var, 1)[0]
        elif dist_val == 'poisson':
            if temp_var == 0:
                self._perf = mean_val
            else:
                self._perf = np.random.poisson(mean_val, 1)[0]
        else:
            self._perf = mean_val

        
    def execute(self, task):

        dur = task.ops / self._perf

        if not self._util:
            task.start_time = 0
        else:
            task.start_time = self._util[-1][1]

        task.end_time = task.start_time + dur
        self._util.append([task.start_time, task.end_time])
        self._task_history.append(task.uid)
        task.exec_core = self._uid

    def to_dict(self):

        return {'uid': self._uid,
                'perf': self._perf,
                'util': self._util,
                'task_history': self._task_history
                }

    def from_dict(self, entry):

        self._uid = entry['uid']
        self._perf = entry['perf']
        self._util = entry['util']
        self._task_history = entry['task_history']
