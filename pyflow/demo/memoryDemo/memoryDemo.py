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

