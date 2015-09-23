#!/usr/bin/env python 
#
"""
automation friendly cross-platform tests for pyflow
"""

import os
import sys

scriptDir=os.path.abspath(os.path.dirname(__file__))


def getOptions() :

    from optparse import OptionParser

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage,description="Run all pyflow tests")

    parser.add_option("--nosge",dest="isSkipSge", action="store_true",
                      help="skip SGE testing")

    (options,args) = parser.parse_args()

    if len(args) != 0 :
        parser.print_help()
        sys.exit(2)

    return (options,args)


def main() :
    import subprocess

    (options,args) = getOptions()

    pyflowRootDir=os.path.abspath(os.path.join(scriptDir,os.pardir,os.pardir))
    pyflowDir=os.path.join(pyflowRootDir,"pyflow")

    utScriptPath=os.path.join(scriptDir,"pyflow_unit_tests.py")

    if True :
        # process-out to run the unit tests for now -- TODO: can we just import this instead?
        utCmd=[sys.executable,"-E",utScriptPath,"-v"]
        proc = subprocess.Popen(utCmd)
        proc.wait()
        if proc.returncode != 0 :
            raise Exception("Pyflow unit test run failed") 

    # run through demos (only helloWorld is working on windows)
    if True :
        demoDir=os.path.join(pyflowDir,"demo")
        for demoName in ["helloWorld"] :
            demoScriptPath=os.path.join(demoDir,demoName,demoName+".py")
            demoCmd=[sys.executable,"-E",demoScriptPath]
            proc = subprocess.Popen(demoCmd)
            proc.wait()
            if proc.returncode != 0 :
                raise Exception("Pyflow demo failed: '%s'" % (demoScriptPath))


main()

