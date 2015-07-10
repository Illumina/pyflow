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
# demonstrate/test addTask() env option
#

import os.path
import sys

# add module path by hand
#
scriptDir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir + "/../../src")

from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class EnvWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :
        # run a task with the parent env:
        #
        home = os.environ["HOME"]
        self.addTask("testDefEnv", "[ $HOME == '%s' ]; exit $?" % (home))

        # create a minimal test environment
        #
        new_path = "/bin"
        min_env = { "PATH" : new_path }
        self.addTask("testMinEnv", "[ $PATH == '%s' ]; exit $?" % (new_path), env=min_env)

        # augment parent env with additional settings:
        #
        augmented_env = os.environ.copy()
        augmented_env["FOO"] = "BAZ"
        self.addTask("testAugmentedEnv", "[ $FOO == 'BAZ' ]; exit $?", env=augmented_env)

        # test funny characters that have shown to cause trouble on some sge installations
        funky_env = {}
        funky_env["PATH"] = "/bin"
        funky_env["_"] = "| %s %F \n"
        # in this case we just want the job to run at all:
        self.addTask("testFunkyEnv", "echo 'foo'; exit $?", env=funky_env)

        assert("FOO" not in os.environ)



# Instantiate the workflow
#
wflow = EnvWorkflow()

# Run the worklow:
#
retval = wflow.run(mode="local")

sys.exit(retval)

