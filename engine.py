import radical.utils as ru
from components.selector import Selector
from components.mapper import Mapper


class Engine(object):

    def __init__(self, workload, resource):

        self._uid = ru.generate_id('engine.%(item_counter)04d', ru.ID_CUSTOM)
        self._workload = workload
        self._resource = resource
        self._mapp = None

    def run(self):

        sel = Selector('all')
        wl_sel = sel.select(self._workload.workload, len(self._workload.workload))
        res_sel = sel.select(self._resource.resource, len(self._resource.resource))

        m = Mapper()
        self._mapp = m.bind(wl_sel, res_sel)


    def get_map(self):
        return self._mapp
