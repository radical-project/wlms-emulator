import radical.utils as ru
from mapping_algos import *

class Mapper(object):

    def __init__(self, algorithm=None):

        self._uid = ru.generate_id('mapper.%(item_counter)04d', ru.ID_CUSTOM)
        self._algo_candidates = [None, 'tte', 'util']

        self._algo = algorithm
        self._mapp = None

    @property
    def mapp(self):
        return self._mapp

    @property
    def algorithm(self):
        return self._algo

    def bind(self, workload, resource):

        if self._algo is None:
            self._mapp = round_robin(workload, resource)

        elif self._algo == 'tte':
            self._mapp = optimize_tte(workload, resource)

        elif self._algo == 'util':
            self._mapp = optimize_util(workload, resource)

        return self._mapp