#!/usr/bin/env python

import os.path
import sys

# add module path by hand
#
scriptDir=os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir+"/../../pyflow/src")

from pyflow import WorkflowRunner



class SimpleWorkflow(WorkflowRunner) :
    """
    A workflow designed to differentiate the runtime impact
    of STREL-391
    """

    def __init__(self) :
        pass

    def workflow(self) :
        for i in range(4000) :
            self.addTask("task%s" % (i),["sleep","0"])



# Instantiate the workflow
#
# parameters are passed into the workflow via its constructor:
#
wflow = SimpleWorkflow()

# Run the worklow:
#
retval=wflow.run(mode="local",nCores=400,isQuiet=True)

sys.exit(retval)

