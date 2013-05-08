#!/usr/bin/env python
#
# Copyright (c) 2012-2013 Illumina, Inc.
#
# This software is provided under the terms and conditions of the
# Illumina Open Source Software License 1.
#
# You should have received a copy of the Illumina Open Source
# Software License 1 along with this program. If not, see
# <https://github.com/downloads/sequencing/licenses/>.
#

#
# This is a very simple demo/test of pyFlow's new (@ v0.4) memory
# resource feature.
#

import os.path
import sys

# add module path by hand
#
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/../../src")


from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from pyflow.WorkflowRunner:
#
class MemTestWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # Each task has a default memory request of 2048 megabytes
        # but this is site-configurable in pyflowConfig.py, so we
        # specify it for every task here
        #
        # This works correctly if task 4 is the only task run in
        # parallel with one of the other 3 tasks.
        #
        self.addTask("task1", "echo 'Hello World!'", memMb=2048)
        self.addTask("task2", "echo 'Hello World!'", memMb=2048)
        self.addTask("task3", "echo 'Hello World!'", memMb=2048)
        self.addTask("task4", "echo 'Hello World!'", memMb=1)



# Instantiate the workflow
#
wflow = MemTestWorkflow()

# Run the worklow:
#
retval = wflow.run(nCores=8, memMb=2049)

# done!
sys.exit(retval)

