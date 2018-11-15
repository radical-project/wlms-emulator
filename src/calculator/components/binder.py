import radical.utils as ru
from .algorithms.mapping_algos import *
from ..exceptions import *

class Binder(object):

    def __init__(self, metric='tte'):

        self._uid = ru.generate_id('maper')
        self._metric_candidates = ['tte', 'util']

        if metric not in self._metric_candidates:
            raise CalcMissingError(obj=self._uid, missing_attribute='metric')
        self._metric = metric

        self._schedule = None

    @property
    def schedule(self):
        return self._schedule

    @property
    def metric(self):
        return self._metric

    def bind(self, workload, resource):

        if self._metric is None:
            self._schedule = round_robin(workload, resource)

        elif self._metric == 'tte':
            self._schedule = optimize_tte(workload, resource)

        elif self._metric == 'util':
            self._schedule = optimize_util(workload, resource)

        return self._map