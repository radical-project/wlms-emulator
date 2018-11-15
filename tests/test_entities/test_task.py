from calculator.entities.node import Node
from calculator.entities.task import Task

def test_task_init():

    t = Task()
    assert t.uid.split('.')[0] == 'task'
    assert t.ops == 0
    assert t.start_time == None
    assert t.end_time == None
    assert t.exec_node == None

    t = Task(ops=100)
    assert t.ops == 100
    t.ops = 200
    assert t.ops == 200

def test_node_execute():

    n = Node(perf=10)
    t = Task(ops=100)
    n.task = t
    n.execute()

    assert t.start_time
    assert t.end_time
    assert t.exec_node == n.uid