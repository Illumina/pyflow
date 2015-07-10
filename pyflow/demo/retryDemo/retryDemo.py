#!/usr/bin/env python
#
# pyFlow - a lightweight parallel task engine
#
# Copyright (c) 2012-2015 Illumina, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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


