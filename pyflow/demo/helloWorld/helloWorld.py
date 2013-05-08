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
# This demo shows possibly the simplist possible pyflow we can create --
# a single 'hello world' task. After experimenting with this file
# please see the 'simpleDemo' for coverage of a few more pyflow features
#

import os.path
import sys

# add module path by hand
#
sys.path.append(os.path.abspath(os.path.dirname(__file__)) + "/../../src")
from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from pyflow.WorkflowRunner:
#
class HelloWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        #
        # The output for this task will be written to the file helloWorld.out.txt
        #
        self.addTask("easy_task1", "echo 'Hello World!' >| helloWorld.out.txt")



# Instantiate the workflow
#
wflow = HelloWorkflow()

# Run the worklow:
#
retval = wflow.run()

# done!
sys.exit(retval)

