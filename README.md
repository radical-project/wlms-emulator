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

The example can be executed by following three steps:

1. Start the executor

The executor can be started using the config file provided in the examples
folder. The executor executes a given schedule and records the execution in
a JSON file.

In a new terminal, load the conda environment and execute:
```
source activate ve_calc
cd $HOME/wlms_calculator/examples
RADICAL_EXECUTOR_VERBOSE=INFO start-executor --cfg_path ./config.yml
```

2. Start the WLMS

The WLMS can be started using the config file provided in the examples
folder. The WLMS, upon receiving a workload and a resource, selects tasks
from the workload, selects a subset of resources from all available, and binds
the tasks to cores (subset of resources) to create a schedule. This schedule is
communicated to an executor.

In a new terminal, load the conda environment and execute:
```
source activate ve_calc
cd $HOME/wlms_calculator/examples
RADICAL_EXECUTOR_VERBOSE=INFO start-wlms --cfg_path ./config.yml
```

3. Create and submit the workload and resource

In the runme Python file in the examples folder, we create a workload and 
resource and submit it to an Engine object. This workload is executed on the
resources by the executor as specified by the WLMS. You can see the various
events by the verbose messages in the different terminals.

In a new terminal, load the conda environment and execute:
```
source activate ve_calc
cd $HOME/wlms_calculator/examples
RADICAL_EXECUTOR_VERBOSE=INFO python runme.py
```