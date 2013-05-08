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
scriptDir=os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir+"/../../src")

from pyflow import WorkflowRunner


#
# test and demostrate the use of a custom success message
# at the end of a workflow
#



# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class SuccessWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # provide a minimum task
        self.addTask("task1","touch success\! && exit 0")




# Instantiate the workflow
#
wflow = SuccessWorkflow()

# Run the worklow:
#
cwd=os.getcwd()
successMsg  = "SuccessWorkflow has successfully succeeded!\n"
successMsg += "\tPlease find your token of successful succeeding here: '%s'\n" % (cwd)
retval=wflow.run(mode="local",nCores=8,successMsg=successMsg,mailTo="csaunders@illumina.com")

sys.exit(retval)

