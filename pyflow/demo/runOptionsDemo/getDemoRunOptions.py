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


import os.path
import sys

pyflowDir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src"))
sys.path.append(pyflowDir)

from optparse import OptionParser, SUPPRESS_HELP
from pyflow import WorkflowRunner

from pyflow import isLocalSmtp


localDefaultCores = WorkflowRunner.runModeDefaultCores('local')
sgeDefaultCores = WorkflowRunner.runModeDefaultCores('sge')



def getDemoRunOptions() :
    """
    This routine is shared by a demo programs to demostrate how to pass pyflow's runtime options on to command-line options. It is not intended to be a demo program itself.
    """

    parser = OptionParser()


    parser.add_option("-m", "--mode", type="string", dest="mode",
                      help="Select run mode {local,sge} (required)")
    parser.add_option("-q", "--queue", type="string", dest="queue",
                      help="Specify sge queue name. Argument ignored if mode is not sge")
    parser.add_option("-j", "--jobs", type="string", dest="jobs",
	                  help="Number of jobs, must be an integer or 'unlimited' (default: %s for local mode, %s for sge mode)" % (localDefaultCores, sgeDefaultCores))
    parser.add_option("-g", "--memGb", type="string", dest="memGb",
	               help="Gigabytes of memory available to run workflow -- only meaningful in local mode, must be an integer or 'unlimited' (default: 2*jobs for local mode, 'unlimited' for sge mode)")
    parser.add_option("-r", "--resume", dest="isResume", action="store_true", default=False,
                      help="Resume a workflow from the point of interuption. This flag has no effect on a new workflow run.")

    isEmail = isLocalSmtp()
    emailHelp=SUPPRESS_HELP
    if isEmail:
        emailHelp="Send email notification of job completion status to this address (may be provided multiple times for more than one email address)"

    parser.add_option("-e", "--mailTo", type="string", dest="mailTo", action="append",
                      help=emailHelp)


    (options, args) = parser.parse_args()

    if not isEmail :
        options.mailTo = None

    if len(args) :
        parser.print_help()
        sys.exit(2)

    if options.mode is None :
        parser.print_help()
        sys.stderr.write("\n\nERROR: must specify run mode\n\n")
        sys.exit(2)
    elif options.mode not in ["local", "sge"] :
        parser.error("Invalid mode. Available modes are: local, sge")

    if options.jobs is None :
        if options.mode == "sge" :
            options.jobs = sgeDefaultCores
        else :
            options.jobs = localDefaultCores
    if options.jobs != "unlimited" :
        options.jobs = int(options.jobs)
        if options.jobs <= 0 :
            parser.error("Jobs must be 'unlimited' or an integer greater than 1")

    # note that the user sees gigs, but we set megs
    if options.memGb is None :
        if options.mode == "sge" :
            options.memMb = "unlimited"
        else :
            if options.jobs == "unlimited" :
                options.memMb = "unlimited"
            else :
                options.memMb = 2 * 1024 * options.jobs
    elif options.memGb != "unlimited" :
        options.memGb = int(options.memGb)
        if options.memGb <= 0 :
            parser.error("memGb must be 'unlimited' or an integer greater than 1")
        options.memMb = 1024 * options.memGb
    else :
        options.memMb = options.memGb

    options.schedulerArgList = []
    if options.queue is not None :
        options.schedulerArgList = ["-q", options.queue]

    return options



if __name__ == "__main__" :
    help(getDemoRunOptions)

