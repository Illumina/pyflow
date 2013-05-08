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
class RetryWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # this task behaves correctly it retries the job 4 times before failing, no automated way
        # to confirm success right now.
        #
        self.flowLog("****** NOTE: This demo is supposed to fail ******")
        self.addTask("retry_task_success", "exit 0", retryMax=8, retryWait=2, retryWindow=0, retryMode="all")
        self.addTask("retry_task_fail", "exit 1", retryMax=3, retryWait=2, retryWindow=0, retryMode="all")



# Instantiate the workflow
#
wflow = RetryWorkflow()

# Run the worklow:
#
retval = wflow.run()

if retval == 0 :
    raise Exception("Example workflow is expected to fail, but did not.")
else :
    sys.stderr.write("INFO: Demo workflow failed as expected.\n\n")


# Run the workflow again to demonstrate that global settings are overridden by task retry settings:
#
retval = wflow.run(retryMax=0)

if retval == 0 :
    raise Exception("Example workflow is expected to fail, but did not.")
else :
    sys.stderr.write("INFO: Demo workflow failed as expected.\n\n")


