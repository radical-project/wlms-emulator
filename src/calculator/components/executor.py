import radical.utils as ru

class Executor(object):

    def __init__(self, schedule):

        self._uid = ru.generate_id('executor')
        self._schedule = schedule
        self._tte = 0
        self._util = 0
        self._profile = list()

        self._execute()


    @property
    def schedule(self):
        return self._schedule

    def _execute(self):

        tasks = list()
        units = list()

        for s in self._schedule:
            tasks.append(s.keys()[0])
            units.append(s.values()[0])

        max_conc = len(set(units))
        cur_conc = 0

        copy_tasks = tasks
        while True:
            if not len(tasks):
                break
            tasks = copy_tasks
            for t in tasks:
                if t.state == 'DONE':
                    copy_tasks.remove(t)
                    cur_conc -= 1
                elif t.state == 'NEW' and cur_conc < max_conc:
                    unit = units[tasks.index(t)]
                    if unit.available:
                        unit.execute(t)
                        cur_conc += 1
