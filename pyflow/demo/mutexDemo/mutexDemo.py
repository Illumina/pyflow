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


import os.path
import sys

# add module path by hand
#
scriptDir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir + "/../../src")

from pyflow import WorkflowRunner


#
# very simple task scripts called by the demo:
#
testJobDir = os.path.join(scriptDir, "testtasks")

sleepjob = os.path.join(testJobDir, "sleeper.bash")  # sleeps



# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class MutexWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # create an array of mutex restricted tasks which can only run
        # once at a time:
        for i in range(8) :
            self.addTask("mutex_task_" + str(i), sleepjob + " 1", mutex="test")

        # and add an array of 'normal' tasks for comparison:
        for i in range(16) :
            self.addTask("normal_task_" + str(i), sleepjob + " 1")




def main() :
    # Instantiate the workflow
    wflow = MutexWorkflow()

    # Run the worklow:
    retval = wflow.run(mode="local", nCores=6)

    sys.exit(retval)



if __name__ == "__main__" :
    main()
