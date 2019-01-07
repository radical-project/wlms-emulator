from calculator.entities.core import Core
from calculator.entities.task import Task

def test_issue_7():

    t1 = Task(ops=100)
    t2 = Task(ops=150)
    t3 = Task(ops=200)
    core = Core(perf=10)

    core.execute(t1)
    core.execute(t2)
    core.execute(t3)

    assert t2.start_time >= t1.start_time
    assert t3.start_time >= t2.start_time


