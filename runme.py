from calculator import Workload
from calculator import Resource
from calculator import Engine
from pprint import pprint

if __name__ == '__main__':

    wl = Workload(size=10, dist='uniform', value=10, var=4)
    res = Resource(size=5, dist='uniform', value=3, var=1)

    print 'Selector: all | Binder: None'
    eng = Engine(wl, res)
    eng.run()
    eng.print_profile()
    print 'TTE: ',eng.get_tte()
    print 'Util: ', eng.get_util()

    # print 'Selector: all | Mapper: tte'
    # wl.reset()
    # res.reset()
    # eng = Engine(wl, res, selector='all', mapper='tte')
    # eng.run()
    # eng.print_profile()
    # print 'TTE: ', eng.get_tte()
    # print 'Util: ', eng.get_util()

    # print 'Selector: all | Mapper: util'
    # wl.reset()
    # res.reset()
    # eng = Engine(wl, res, selector='all', mapper='util')
    # eng.run()
    # eng.print_profile()
    # print 'TTE: ', eng.get_tte()
    # print 'Util: ', eng.get_util()
