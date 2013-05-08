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
# demonstrate/test addTask() cwd option
#

import os.path
import sys

# add module path by hand
#
scriptDir=os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir+"/../../src")

from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class CwdWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # get cwd and its parent for the addTask cwd test
        #
        cwd=os.getcwd()
        parentdir=os.path.abspath(os.path.join(cwd,".."))

        self.flowLog("testing pyflow cwd: '%s' parentdir: '%s'" % (cwd,parentdir))

        # task will fail unless pwd == parentdir:
        #
        # test both absolute and relative cwd arguments:
        #
        self.addTask("testAbsCwd","[ $(pwd) == '%s' ]; exit $?" % (parentdir),cwd=parentdir)
        self.addTask("testRelCwd","[ $(pwd) == '%s' ]; exit $?" % (parentdir),cwd="..")



# Instantiate the workflow
#
wflow = CwdWorkflow()

# Run the worklow:
#
retval=wflow.run(mode="local")

sys.exit(retval)

