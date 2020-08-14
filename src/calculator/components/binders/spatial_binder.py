import radical.utils as ru
from ..algorithms.spatial_binding_algos import round_robin, largest_to_fastest, largest_to_fastest_alt, smallest_to_fastest, random_placer
from ...exceptions import CalcValueError


class Spatial_Binder(object):

    def __init__(self):
        self._uid = ru.generate_id('binder')
        self._criteria_options = ['rr', 'l2f', 'l2f_alt', 's2f', 'random']
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

    def bind(self, workload, resource):
        if not self._criteria:
            raise CalcValueError(obj=self._uid, attribute='criteria',
                                 expected_value=self._criteria_options,
                                 actual_value=None)

        if self._criteria == 'rr':
            self._schedule = round_robin(workload, resource)

        elif self._criteria == 'l2f':
            self._schedule = largest_to_fastest(workload, resource)

        elif self._criteria == 'l2f_alt':
            self._schedule = largest_to_fastest_alt(workload, resource)

        elif self._criteria == 's2f':
            self._schedule = smallest_to_fastest(workload, resource)

        elif self._criteria == 'random':
            self._schedule = random_placer(workload, resource)

        return self._schedule
