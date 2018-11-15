import radical.utils as ru
import numpy as np
from ..entities.node import Node
from ..exceptions import *


class Resource(object):

    def __init__(self, num_nodes=0, perf_dist='uniform', dist_value=None, dist_var=None):

        # Initialize
        self._uid = ru.generate_id('resource')
        self._dist_candidates = ['uniform', 'normal']
        self._node_list = list()

        if not num_nodes and not perf_dist \
                and not dist_value and not dist_var:
            raise CalcMissingError(obj=self._uid,
                                   missing_attribute='num_nodes|perf_dist|dist_value|dist_var')

        if perf_dist not in self._dist_candidates:
            raise ValueError("possible distributions are %s" %
                             (','.join(self._dist_candidates)))

        self._num_nodes = num_nodes
        self._perf_dist = perf_dist
        self._dist_value = dist_value
        self._dist_var = dist_var

        self._create_node_list()

    @property
    def num_nodes(self):
        return self._num_nodes

    @property
    def node_list(self):
        return self._node_list

    def _create_node_list(self):

        # Select N samples from the selected distribution
        if self._perf_dist == 'uniform':
            samples = np.random.uniform(low=self._dist_value - self._dist_var, high=self._dist_value + self._dist_var,
                                        size=self._num_nodes)
        elif self._perf_dist == 'normal':
            samples = [self._dist_value*np.random.randn()+self._dist_var for _ in range(self._num_nodes)]

        # Create N execution units with the selected samples
        self._node_list = [Node(samples[i]) for i in range(self._num_nodes)]

    def reset(self):

        for n in self._node_list:
            n.task = None
            n.util = list()
