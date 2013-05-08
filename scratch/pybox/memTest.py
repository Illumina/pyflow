#!/usr/bin/env python

#
# This demo shows possibly the simplist possible pyflow we can create -- 
# a single 'hello world' task. After experimenting with this file
# please see the 'simpleDemo' for coverage of a few more pyflow features
#

import os.path
import sys

# add module path by hand
#
sys.path.append(os.path.abspath(os.path.dirname(__file__))+"/../pyflow/src")


from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from pyflow.WorkflowRunner:
#
class MemTestWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # The following is our first and only task for this workflow:
        self.addTask("easy_task1","echo 'Hello World!'")
        self.addTask("easy_task2","echo 'Hello World!'")
        self.addTask("easy_task3","echo 'Hello World!'")
        self.addTask("easy_task4","echo 'Hello World!'",memMb=1)



# Instantiate the workflow
#
wflow = MemTestWorkflow()

# Run the worklow:
#
retval=wflow.run(nCores=8,memMb=2049)

# done!
sys.exit(retval)

