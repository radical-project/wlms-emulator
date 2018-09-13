from components.workload import Workload
from components.resource import Resource
from engine import Engine
from pprint import pprint

if __name__ == '__main__':

    wl = Workload(size=10)
    res = Resource(size=5)

    eng = Engine(wl, res)
    eng.run()
    pprint (eng.get_map())