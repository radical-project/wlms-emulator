import radical.utils as ru
import numpy as np
from ..entities.core import Core
from ..exceptions import *


class Resource(object):

    def __init__(self, num_cores=1, perf_dist='uniform', dist_mean=10, dist_var=0, no_uid=False):

        # Initialize
        self._uid = None
        self._dist_options = ['uniform', 'normal']
        self._core_list = list()

        if not isinstance(num_cores, int):
            raise CalcTypeError(expected_type=int,
                                actual_type=type(num_cores),
                                entity='num_cores'
                            )
        if not isinstance(perf_dist, str):
            raise CalcTypeError(expected_type=str,
                                actual_type=type(num_cores),
                                entity='perf_dist'
                            )

        if perf_dist not in self._dist_options:
            raise ValueError("possible distributions are %s" %(','.join(self._dist_options)))


        if not (isinstance(dist_mean, int) or isinstance(dist_mean, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(dist_mean),
                                entity='dist_mean'
                            )

        if not (isinstance(dist_var, int) or isinstance(dist_var, float)):
            raise CalcTypeError(expected_type='int or float',
                                actual_type=type(dist_var),
                                entity='dist_var'
                            )

        self._num_cores = num_cores
        self._perf_dist = perf_dist
        self._dist_mean = dist_mean
        self._dist_var = dist_var

        if not no_uid:
            self._uid = ru.generate_id('resource')
            self._create_core_list()

    @property
    def uid(self):
        return self._uid

    @property
    def num_cores(self):
        return self._num_cores

    @property
    def core_list(self):
        return self._core_list

    def _create_core_list(self):

        # Select N samples from the selected distribution
        if self._perf_dist == 'uniform':
            samples = np.random.uniform(low=self._dist_mean - self._dist_var, high=self._dist_mean + self._dist_var,
                                        size=self._num_cores)
        elif self._perf_dist == 'normal':
            samples = [self._dist_mean*np.random.randn()+self._dist_var for _ in range(self._num_cores)]

        # Create N execution units with the selected samples
        self._core_list = [Core(samples[i]) for i in range(self._num_cores)]


    def to_dict(self):

        core_list_as_dict = list()
        for core in self._core_list:
            core_list_as_dict.append(core.to_dict())

        return {
                'uid': self._uid,
                'num_cores': self._num_cores,
                'perf_dist': self._perf_dist,
                'dist_mean': self._dist_mean,
                'dist_var': self._dist_var,
                'core_list': core_list_as_dict
            }

    def from_dict(self, entry):

        self._uid       = entry['uid']
        self._num_cores = entry['num_cores']
        self._perf_dist = entry['perf_dist']
        self._dist_mean = entry['dist_mean']
        self._dist_var  = entry['dist_var']

        for core in entry['core_list']:
            c = Core(no_uid=True)
            c.from_dict(core)
            self._core_list.append(c)

    def reset(self):

        for n in self._core_list:
            n.task = None
            n.util = list()
