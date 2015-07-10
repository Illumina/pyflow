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

"""
pyflowConfig

This file defines a class instance 'siteConfig' containing pyflow components
which are the most likely to need site-specific configuration.
"""

import os


# this is the only object pyflow needs to import, it
# is defined at the end of this module:
#
siteConfig = None


# depending on network setup getfqdn() can be slow, so cache calls to this function here:
#
def _getHostName() :
    import socket
    return socket.getfqdn()

cachedHostName = None


def getHostName() :
    global cachedHostName
    if cachedHostName is None :
        cachedHostName = _getHostName()
    return cachedHostName


def getDomainName() :
    hn = getHostName().split(".")
    if len(hn) > 1 : hn = hn[1:]
    return ".".join(hn)



class DefaultSiteConfig(object) :
    """
    Default configuration settings are designed to work with as
    many sites as technically feasible
    """

    # All email notifications come from the following e-mail address
    #
    mailFrom = "pyflow-bot@" + getDomainName()

    # Default memory (in megabytes) requested by each command task:
    #
    defaultTaskMemMb = 2048

    # In local run mode, this is the defalt memory per thread that we
    # assume is available:
    #
    defaultHostMemMbPerCore = 2048

    # maximum number of jobs which can be submitted to sge at once:
    #
    # allowed values are "unlimited" or None for unlimited jobs, or
    # a positive integer describing the max number of jobs
    #
    maxSGEJobs = 128

    # both getHostName and getDomainName are used in the
    # siteConfig factory, so these are not designed to be
    # overridden at present:
    getHostName = staticmethod(getHostName)
    getDomainName = staticmethod(getDomainName)

    @classmethod
    def qsubResourceArg(cls, nCores, memMb) :
        """
        When a task is launched using qsub in sge mode, it will call this
        function to specify the requested number of threads and megabytes
        of memory. The returned argument list will be appended to the qsub
        arguments.

        nCores -- number of threads requested
        memMb -- memory requested (in megabytes)
        """
        nCores = int(nCores)
        memMb = int(memMb)
        return cls._qsubResourceArgConfig(nCores, memMb)

    @classmethod
    def _qsubResourceArgConfig(cls, nCores, memMb) :
        """
        The default function is designed for maximum
        portability  -- it just provides more memory
        via more threads.
        """

        # this is the memory we assume is available per
        # thread on the cluster:
        #
        class Constants(object) : megsPerCore = 4096

        memCores = 1 + ((memMb - 1) / Constants.megsPerCore)

        qsubCores = max(nCores, memCores)

        if qsubCores <= 1 : return []
        return ["-pe", "threaded", str(qsubCores)]


    @classmethod
    def getSgeMakePrefix(cls, nCores, memMb, schedulerArgList) :
        """
        This prefix will be added to ' -C directory', and run from
        a local process to handle sge make jobs.

        Note that memMb hasn't been well defined for make jobs yet,
        is it the per task memory limit? The first application to
        accually make use of this will have to setup the convention,
        it is ignored right now...
        """
        nCores = int(nCores)
        memMb = int(memMb)

        retval = ["qmake",
                "-V",
                "-now", "n",
                "-cwd",
                "-N", "pyflowMakeTask"]

        # user arguments to run() (usually q specification:
        retval.extend(schedulerArgList)

        #### use qmake parallel environment:
        # retval.extend(["-pe","make",str(nCores),"--"])

        #### ...OR use 'dynamic' sge make environment:
        retval.extend(["--", "-j", str(nCores)])

        return retval



def getEnvVar(key) :
    if key in os.environ : return os.environ[key]
    return None



#
# this is site specific configuration information, although it won't be triggered
# for your site, it provides an example of how SGE mode can be customized:
#

class ChukSiteConfig(DefaultSiteConfig) :
    """
    Chuk environment specializations..
    """

    sgeRoot = getEnvVar('SGE_ROOT')

    @classmethod
    def _qsubResourceArgConfig(cls, nCores, memMb) :

        if cls.sgeRoot is None :
            raise Exception("Can't find expected environment variable: 'SGE_ROOT'")

        retval = []

        # this stack size setting is required for multi-core jobs,
        # and a good idea just in case for other jobs:
        retval.extend(["-l", "s_stack=10240k"])

        # in the uk we can specify our memory requirements:
        memGb = 1 + ((memMb - 1) / 1024)
        reqMemFile = "%s/req/mem%iG" % (cls.sgeRoot, memGb)
        assert os.path.isfile(reqMemFile)
        retval.extend(["-@", reqMemFile])

        if nCores > 1 :
            retval.extend(["-pe", "threaded", str(nCores)])

        return retval



class Ilmn2012Config(DefaultSiteConfig) :
    """
    The new standard cluster configuration at ilmn
    """

    @classmethod
    def _qsubResourceArgConfig(cls, nCores, memMb) :

        retval = []

        # specify memory requirements
        memGb = 1 + ((memMb - 1) / 1024)
        reqArg = "h_vmem=%iG" % (memGb)
        retval.extend(["-l", reqArg])

        if nCores > 1 :
            retval.extend(["-pe", "threaded", str(nCores)])

        return retval



#
# final step is the selection of this run's siteConfig object:
#

def siteConfigFactory() :
    isChukConfig = (DefaultSiteConfig.getDomainName() == "chuk.illumina.com")
    isIlmn2012Config = False
    if DefaultSiteConfig.getHostName().startswith("uscp-prd-ln") :
        isIlmn2012Config = True
#    if DefaultSiteConfig.getHostName().startswith("ussd-prd-ln") :
#        isIlmn2012Config=True

    if isChukConfig :
        return ChukSiteConfig
    elif isIlmn2012Config :
        return Ilmn2012Config
    else :
        return DefaultSiteConfig


siteConfig = siteConfigFactory()


