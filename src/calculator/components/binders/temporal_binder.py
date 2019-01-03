import radical.utils as ru
from ..algorithms.binding_algos import round_robin, optimize_tte, optimize_util, random_placer
from ...exceptions import CalcValueError


class Temporal_Binder(object):

    def __init__(self):

        self._uid = ru.generate_id('binder')
        self._criteria_options = ['rr', 'tte', 'util', 'random']
        self._criteria = None
        self._schedule = None

    @property
    def schedule(self):
        return self._schedule

    @property
    def criteria(self):
        return self._criteria

    @criteria.setter
    def criteria(self, val):
        if val not in self._criteria_options:
            raise CalcValueError(obj=self._uid, attribute='criteria',
                                 expected_value=self._criteria_options,
                                 actual_value=val)

        self._criteria = val

    def bind(self, workload, resource, submit_time):

        if not self._criteria:
            raise CalcValueError(obj=self._uid, attribute='criteria',
                                 expected_value=self._criteria_options,
                                 actual_value=None)

        if self._criteria == 'rr':
            self._schedule = round_robin(workload, resource)

        elif self._criteria == 'tte':
            self._schedule = optimize_tte(workload, resource)

        elif self._criteria == 'util':
            self._schedule = optimize_util(workload, resource)

        elif self._criteria == 'random':
            self._schedule = random_placer(workload, resource)

        return self._schedule
