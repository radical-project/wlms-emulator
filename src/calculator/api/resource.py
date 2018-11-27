import radical.utils as ru
import numpy as np
from ..entities.node import Node
from ..exceptions import *


class Resource(object):

    def __init__(self, num_cores=1, perf_dist='uniform', dist_mean=None, dist_var=None):

        # Initialize
        self._uid = ru.generate_id('resource')
        self._dist_options = ['uniform', 'normal']
        self._node_list = list()

        if not num_cores and not perf_dist \
                and not dist_mean and not dist_var:
            raise CalcMissingError(obj=self._uid,
                                   missing_attribute='num_cores|perf_dist|dist_mean|dist_var')

        if perf_dist not in self._dist_options:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_options)))

        self._num_cores = num_cores
        self._perf_dist = perf_dist
        self._dist_mean = dist_mean
        self._dist_var = dist_var

        self._create_node_list()

    @property
    def num_cores(self):
        return self._num_cores

    @property
    def node_list(self):
        return self._node_list

    def _create_node_list(self):

        # Select N samples from the selected distribution
        if self._perf_dist == 'uniform':
            samples = np.random.uniform(low=self._dist_mean - self._dist_var, high=self._dist_mean + self._dist_var,
                                        size=self._num_cores)
        elif self._perf_dist == 'normal':
            samples = [self._dist_mean*np.random.randn()+self._dist_var for _ in range(self._num_cores)]

        # Create N execution units with the selected samples
        self._node_list = [Node(samples[i]) for i in range(self._num_cores)]

    def reset(self):

        for n in self._node_list:
            n.task = None
            n.util = list()
