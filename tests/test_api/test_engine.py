from calculator.api.engine import Engine
from calculator.api.resource import Resource
from calculator.api.workload import Workload
from calculator.exceptions import *
from calculator.entities.node import Node
from calculator.entities.task import Task

def test_engine_init():
    Engine