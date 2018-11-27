# WLMS Calculator

This document discusses the installation and execution instructions
for the WLMS calculator.

* Assumptions:
  * No noise from resource
  * No noise in WLMS
  * Tasks are single-core
  * No notion of nodes, only cores
  * Each task has to perform a certain number of operations
  * Each core can do a certain number of operations per second

SRS Document: [Link](ttps://docs.google.com/document/d/1g--pYhwbrSz8m2XrJJcEpc3w8EcNSufa_GrJhrKdfn4/edit)

## Installation

```
conda create -n ve_calc
source activate ve_calc
cd $HOME
git clone https://github.com/vivek-bala/wlms_calculator.git
cd wlms_calculator
pip install .
```

## Executing the example

The example can be executed with the following commands:

```
cd $HOME/wlms_calculator/examples
python runme.py
```

The output printed describes the execution profile in the following
format:

```
Task: task.0 | Core: <core used to execute the task> | Start time: <time execution started> | End time: <time execution stopped> | Execution time: <execution time>
Task: task.1 | Core: <core used to execute the task> | Start time: <time execution started> | End time: <time execution stopped> | Execution time: <execution time>
```

## Adding selection and binding algorithms

Currently, there are 2-3 trivial selection and binding algorithms as part of
the package. An API will be added to enable addition of algorithms via the
user interface,